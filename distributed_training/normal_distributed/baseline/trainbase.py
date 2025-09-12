# train_ddp_mnist_int8.py
# -*- coding: utf-8 -*-
import os, time, torch, torch.nn as nn, torch.optim as optim, torch.nn.functional as F
from torch.utils.data import DataLoader
from torchvision import datasets, transforms

import DDS_All as dds
from ZrddsDenseBroadcast import ZrddsDenseBroadcast
from dgc_stepperBaseline import DDPDGCStepper
from compressionBaseline import Int8Compressor
from memoryBaseline import Int8SGDMemory
from dgc_evalBaseline import ddp_evaluate_top1
from dds_barrier_verboseBaseline import ddp_barrier_verbose

# ---- 环境参数（也可从命令行传入）
RANK      = int(os.environ.get("RANK", "1"))
WORLD     = int(os.environ.get("WORLD_SIZE", "2"))
GROUP     = os.environ.get("GROUP_ID", "job-20250908-01")
DOMAIN_ID = int(os.environ.get("DDS_DOMAIN_ID", "200"))
DATA_DIR  = os.environ.get("DATA_DIR", "../data")

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

def make_loaders(data_dir, batch_size, device):
    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),   # 标准 MNIST 归一化
    ])
    train_ds = datasets.MNIST(root=data_dir, train=True,  download=True, transform=tfm)
    val_ds   = datasets.MNIST(root=data_dir, train=False, download=True, transform=tfm)

    pin = (device.type == "cuda")
    # Windows 下多进程 DataLoader 需要 spawn；不确定就先用 num_workers=0
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True,
                              pin_memory=pin, drop_last=True)
    val_loader   = DataLoader(val_ds,   batch_size=1024,    shuffle=False,
                              num_workers=0, pin_memory=pin)
    return train_loader, val_loader

# ---- 在这里给 ZrddsAllgather 添加轮询函数
def wait_for_discovery(ag: ZrddsDenseBroadcast, world:int, timeout_ms:int=10000, include_self:bool=True, poll_ms:int=200):
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
            print(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}")
            return
        if cw != last_w or cr != last_r:
            print(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}")
            last_w, last_r = cw, cr
        if time.time() >= deadline:
            raise TimeoutError(f"discovery timeout: writer={cw}, reader={cr}, target={target}, world={world}")
        time.sleep(poll_ms/1000.0)

def main():
    # DDS participant
    dp = dds.DomainParticipantFactory.get_instance().create_participant(
        DOMAIN_ID, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
    dds.register_all_types(dp)

    # 通信引擎
    ag = ZrddsDenseBroadcast(dp, topic="ddp/allgather_blob")

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

    # Int8 压缩器
    mem = Int8SGDMemory(momentum=0.9, nesterov=False, gradient_clipping=None)
    comp = Int8Compressor(memory=mem, warmup_epochs=3)
    stepper = DDPDGCStepper(model, comp, ag, GROUP, RANK, WORLD)

    # 数据
    train_loader, val_loader = make_loaders(DATA_DIR, batch_size=256, device=device)
    loss_fn = nn.CrossEntropyLoss()

    # 训练参数
    epochs = 3
    eval_every = 100
    EVAL_ROUND_OFFSET = 1_000_000_000

    global_step = 0
    for ep in range(epochs):
        for xb, yb in train_loader:
            xb = xb.to(device, non_blocking=True)
            yb = yb.to(device, non_blocking=True)

            opt.zero_grad(set_to_none=True)
            stepper.begin_step(global_step)

            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()

            stepper.finish_and_apply(timeout_s=100000)
            opt.step()

            if RANK == 0 and (global_step % 100 == 0):
                print(f"[rank {RANK}] step {global_step} loss={loss.item():.4f}")

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
                    print(f"[VAL] step {global_step:05d} acc={acc*100:.2f}% ({g_correct}/{g_total})")

            global_step += 1

    dp.delete_contained_entities()
    if RANK == 0: print("[train] done.")

if __name__ == "__main__":
    main()