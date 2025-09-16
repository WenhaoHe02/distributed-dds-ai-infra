# train_ddp_mnist.py
# -*- coding: utf-8 -*-
import logging
import os, time, torch, torch.nn as nn, torch.optim as optim, torch.nn.functional as F
import datetime

from sympy.physics.units.definitions.dimension_definitions import information
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms

import DDS_All as dds
from zrdds_allgather import ZrddsAllgather
from dgc_stepper import DDPDGCStepper
from compression import DGCCompressor
from memory import DGCSGDMemory
from dgc_eval import ddp_evaluate_top1   # 用你提供的 ddp_eval.py
from dds_barrier_verbose import ddp_barrier_verbose

# ---- 环境参数（也可从命令行传入）
RANK      = int(os.environ.get("RANK", "0"))
WORLD     = int(os.environ.get("WORLD_SIZE", "1"))
GROUP     = os.environ.get("GROUP_ID", "job-20250908-01")
DOMAIN_ID = int(os.environ.get("DDS_DOMAIN_ID", "200"))
DATA_DIR  = os.environ.get("DATA_DIR", "data")

logging.basicConfig(level=logging.INFO)

# ---- 模型：自动扁平化 28x28 -> 784
class MNISTNet(nn.Module):
    def __init__(self, hidden=512, out_dim=10):
        super().__init__()
        self.net = nn.Sequential(
            nn.Flatten(),                 # [B,1,28,28] -> [B,784]
            nn.Linear(784, hidden),
            nn.ReLU(),
            nn.Linear(hidden, out_dim),
        )
    def forward(self, x): return self.net(x)

def make_loaders(data_dir, batch_size, device, subset_size:int = None):
    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),   # 标准 MNIST 归一化
    ])
    train_ds = datasets.MNIST(root=data_dir, train=True,  download=True, transform=tfm)
    val_ds   = datasets.MNIST(root=data_dir, train=False, download=True, transform=tfm)

    # ✅ 子集调试（可选）
    if subset_size is not None:
        train_ds = Subset(train_ds, list(range(min(subset_size, len(train_ds)))))
        val_ds = Subset(val_ds, list(range(min(subset_size, len(val_ds)))))

    pin = (device.type == "cuda")
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True,
                              pin_memory=pin, drop_last=True)
    val_loader   = DataLoader(val_ds,   batch_size=batch_size, shuffle=False,
                              num_workers=0, pin_memory=pin)
    return train_loader, val_loader

# ---- 在这里给 ZrddsAllgather 添加轮询函数
def wait_for_discovery(ag: ZrddsAllgather, world:int, timeout_ms:int=10000, include_self:bool=True, poll_ms:int=200):
    """阻塞直到 discovery 匹配完成"""
    deadline = time.time() + timeout_ms/1000.0
    target = world if include_self else max(0, world-1)

    def _get_pub_count():
        st = ag.writer.get_publication_matched_status()  # 直接返回 DDS_All.PublicationMatchedStatus
        return int(getattr(st, "current_count", 0))

    def _get_sub_count():
        st = ag.reader.get_subscription_matched_status()
        return int(getattr(st, "current_count", 0))

    last_w = last_r = -1
    while True:
        cw, cr = _get_pub_count(), _get_sub_count()
        if (cw >= target) and (cr >= target):
            logging.info(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}")
            return
        if cw != last_w or cr != last_r:
            logging.info(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}")
            last_w, last_r = cw, cr
        if time.time() >= deadline:
            raise TimeoutError(f"discovery timeout: writer={cw}, reader={cr}, target={target}, world={world}")
        time.sleep(poll_ms/1000.0)

def main():
    # DDS participant
    script_start = datetime.datetime.now()
    if RANK == 0:
        logging.info(f"[federal_train_scripts] started at {script_start.strftime('%Y-%m-%d %H:%M:%S')}")
    dp = dds.DomainParticipantFactory.get_instance().create_participant(
        DOMAIN_ID, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
    dds.register_all_types(dp)

    # 通信引擎
    ag = ZrddsAllgather(dp, topic="ddp/allgather_blob")

    # ---- ★ 在 barrier 之前先确保 discovery 已完成
    wait_for_discovery(ag, world=WORLD, timeout_ms=100000, include_self=True)

    ok = ddp_barrier_verbose(ag, group_id=GROUP, rank=RANK, world=WORLD,
                             domain_id=DOMAIN_ID, topic_name="ddp/allgather_blob",
                             min_writer_matches=WORLD, min_reader_matches=WORLD,
                             match_timeout_s=150.0, barrier_timeout_s=600.0)
    if not ok:
        raise SystemExit("[barrier] failed; check missing ranks / matching logs")

    # 模型/优化器
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = MNISTNet().to(device)
    opt = optim.SGD(model.parameters(), lr=0.1, momentum=0.9)

    # 压缩器
    mem = DGCSGDMemory(momentum=0.0, nesterov=False, gradient_clipping=None, momentum_masking=True)
    comp = DGCCompressor(compress_ratio=0.001, memory=mem, fp16_values=True, int32_indices=True, warmup_epochs=3)
    stepper = DDPDGCStepper(model, comp, ag, GROUP, RANK, WORLD)

    # 数据
    train_loader, val_loader = make_loaders(DATA_DIR, batch_size=128, device=device, subset_size=36000)
    loss_fn = nn.CrossEntropyLoss()

    # 训练参数
    epochs = 10
    eval_every = 100
    EVAL_ROUND_OFFSET = 1_000_000_000

    global_step = 0

    # ===== 计时统计 =====
    # 1) 纯通信等待（毫秒累加，来自 finish_and_apply 的 wait-only）
    comm_wait_total_ms = 0.0
    comm_wait_max_ms = 0.0
    # 2) 通信阶段墙钟时间（秒，覆盖 begin_step+finish_and_apply；包含压缩/解压/收集合并）
    comm_phase_wall_total_s = 0.0
    # 3) 训练步数
    n_steps = 0

    train_wall_t0 = time.perf_counter()

    for ep in range(epochs):
        comp.warmup_compress_ratio(ep)
        for xb, yb in train_loader:
            xb = xb.to(device, non_blocking=True)
            yb = yb.to(device, non_blocking=True)

            opt.zero_grad(set_to_none=True)
            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()

            # 正确顺序：backward -> begin_step -> finish_and_apply -> optimizer.step
            phase_t0 = time.perf_counter()
            stepper.begin_step(global_step)
            wait_ms = stepper.finish_and_apply(timeout_s=100000.0)  # 纯 wait（毫秒）
            comm_phase_wall_total_s += (time.perf_counter() - phase_t0)

            comm_wait_total_ms += wait_ms
            if wait_ms > comm_wait_max_ms: comm_wait_max_ms = wait_ms
            n_steps += 1

            opt.step()

            if RANK == 0 and (global_step % 100 == 0):
                logging.info(f"[rank {RANK}] step {global_step} loss={loss.item():.4f}")

            # 按 step 做全局评估（Top-1）
            if (global_step + 1) % eval_every == 0:
                metric_round = EVAL_ROUND_OFFSET + global_step
                g_correct, g_total, acc = ddp_evaluate_top1(
                    model, val_loader, device,
                    zrdds=ag, group_id=GROUP,
                    epoch_or_step=metric_round,
                    name="val.top1", rank=RANK, world=WORLD, timeout_s=100000.0
                )
                if RANK == 0:
                    logging.info(f"[VAL] step {global_step:05d} acc={acc*100:.2f}% ({g_correct}/{g_total})")

            global_step += 1

        # 每个 epoch 结束打印一次 stepper 聚合统计
        stepper.report_and_reset(tag=f"epoch-{ep}")

    train_wall_s = time.perf_counter() - train_wall_t0

    # 清理
    dp.delete_contained_entities()
    script_end = datetime.datetime.now()

    # ===== 结果输出 =====
    if RANK == 0:
        script_duration = script_end - script_start
        avg_wait_ms = (comm_wait_total_ms / max(1, n_steps))
        logging.info(f"[federal_train_scripts] finished at {script_end.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"[federal_train_scripts] total duration: {str(script_duration)}")
        # 纯通信等待
        logging.info(f"[COMM][pure_wait] steps={n_steps} "
                     f"total={comm_wait_total_ms/1000.0:.3f}s  avg={avg_wait_ms:.2f}ms  max={comm_wait_max_ms:.2f}ms")
        # 通信阶段墙钟（begin_step+finish_and_apply）
        logging.info(f"[COMM][phase_wall] steps={n_steps} "
                     f"total={comm_phase_wall_total_s:.3f}s  avg={ (comm_phase_wall_total_s/max(1,n_steps)) :.3f}s")
        # 训练循环墙钟
        logging.info(f"[TRAIN][wall] total_train_loop={train_wall_s:.3f}s")

    if RANK == 0:
        logging.info("[federal_train_scripts] done.")
    print(f"[train] transition time: {comm_phase_wall_total_s:.6f} s")

if __name__ == "__main__":
    main()
