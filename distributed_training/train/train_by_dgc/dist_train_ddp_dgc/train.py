# -*- coding: utf-8 -*-
# 示例：每个 step 通信一次（DDP 语义），用 ZRDDS allgather + DGC 压缩
import os, time
import torch
import torch.nn as nn
import torch.optim as optim

import DDS_All as dds
from zrdds_allgather import ZrddsAllgather
from dgc_stepper import DDPDGCStepper
import compression, memory
import DGCSGD

# ---- 读 rank/world（你也可以从命令行或配置里传）
RANK = int(os.environ.get("RANK", "0"))
WORLD = int(os.environ.get("WORLD_SIZE", "1"))
GROUP = os.environ.get("GROUP_ID", "job-20250908-01")
DOMAIN_ID = int(os.environ.get("DDS_DOMAIN_ID", "200"))

# ---- 你的模型
class Net(nn.Module):
    def __init__(self, in_dim=784, hidden=512, out_dim=10):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(in_dim, hidden),
            nn.ReLU(),
            nn.Linear(hidden, out_dim),
        )
    def forward(self, x): return self.net(x)

def main():
    # DDS participant
    dp = dds.DomainParticipantFactory.get_instance().create_participant(
        DOMAIN_ID, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
    dds.register_all_types(dp)

    # 通信引擎
    ag = ZrddsAllgather(dp, topic="ddp/allgather_blob")

    # 模型/优化器
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = Net().to(device)
    opt = DGCSGD.DGCSGD(model.parameters(), lr=0.1, momentum=0.9)

    # 压缩器（0.1% 稀疏；值用 fp16、索引用 int32 可选）
    mem = memory.DGCSGDMemory(momentum=0.9, nesterov=False, gradient_clipping=None, momentum_masking=True)
    comp = compression.DGCCompressor(compress_ratio=0.001, memory=mem, fp16_values=False, int32_indices=True, warmup_epochs=1)

    # Stepper
    stepper = DDPDGCStepper(model, comp, ag, GROUP, RANK, WORLD)

    # 造点假数据
    bs = 256
    x = torch.randn(bs, 784, device=device)
    y = torch.randint(0, 10, (bs,), device=device)

    loss_fn = nn.CrossEntropyLoss()

    # 训练若干 step
    steps = 20
    for step in range(steps):
        opt.zero_grad(set_to_none=True)
        stepper.begin_step(step)

        logits = model(x)
        loss = loss_fn(logits, y)
        loss.backward()

        # 通信+写回梯度
        stepper.finish_and_apply(timeout_s=120)

        opt.step()

        if RANK == 0 and (step % 5 == 0):
            print(f"[rank {RANK}] step {step} loss={loss.item():.4f}")

    # 清理
    dp.delete_contained_entities()
    print(f"[rank {RANK}] done.")

if __name__ == "__main__":
    main()
