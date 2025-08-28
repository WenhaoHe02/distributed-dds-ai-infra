# client.py  —— 使用“IDL生成的Python类型”的最小DDS客户端
# 依赖：pip install cyclonedds torch torchvision

import argparse, random, time
from typing import List

import torch, torch.nn as nn, torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms
from torch.nn.utils import parameters_to_vector

# === 这里改成你生成模块的导入路径 ===
# 假设你的 ai_train.idl 内容是 module fed { struct TrainCmd/ClientUpdate/ModelBlob; }
# idlc -l py 会生成同名 Python 包/模块，直接这样引入：:contentReference[oaicite:1]{index=1}
from ai_train import TrainCmd, ClientUpdate, ModelBlob   # ← 按你的生成结果调整包名

# Cyclone DDS Python API（Topic / DataReader / DataWriter）:contentReference[oaicite:2]{index=2}
from cyclonedds.domain import DomainParticipant
from cyclonedds.topic import Topic
from cyclonedds.sub import DataReader
from cyclonedds.pub import DataWriter

# ---- 简单的MNIST网络（CPU）----
class Net(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.pool  = nn.MaxPool2d(2)
        self.fc1   = nn.Linear(64*12*12, 128)
        self.fc2   = nn.Linear(128, 10)
    def forward(self, x):
        x = F.relu(self.conv1(x)); x = F.relu(self.conv2(x))
        x = self.pool(x); x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x)); return self.fc2(x)

def params_to_f32_bytes(model: nn.Module) -> bytes:
    vec = parameters_to_vector(model.parameters()).detach().to(torch.float32).contiguous().view(-1)
    return vec.numpy().tobytes()  # 小端字节

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client_id", type=int, required=True)
    ap.add_argument("--data_dir", type=str, default="./data")
    ap.add_argument("--batch_size", type=int, default=128)
    ap.add_argument("--domain", type=int, default=0)
    args = ap.parse_args()

    client_id = int(args.client_id)
    device = torch.device("cpu")

    # === DDS 实体/话题（名字要与控制端一致）===
    dp = DomainParticipant(args.domain)
    t_cmd   = Topic(dp, "train/train_cmd",   TrainCmd)
    t_upd   = Topic(dp, "train/client_update", ClientUpdate)
    t_model = Topic(dp, "train/model_blob",  ModelBlob)

    r_cmd   = DataReader(dp, t_cmd)
    r_model = DataReader(dp, t_model)
    w_upd   = DataWriter(dp, t_upd)

    # === 数据 ===
    tfm = transforms.Compose([transforms.ToTensor(),
                              transforms.Normalize((0.1307,), (0.3081,))])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)

    # === 模型 ===
    model = Net().to(device)

    print(f"[Client {client_id}] started. waiting for TrainCmd ...")

    last_round = -1
    while True:
        # 1) 等命令（按轮次去重）
        cmd = None
        for m in r_cmd.take_iter(timeout=2000):  # 2s 轮询
            if int(m.round_id) > last_round:
                cmd = m; break
        if cmd is None:
            time.sleep(0.05); continue

        round_id   = int(cmd.round_id)
        subset_sz  = int(cmd.subset_size)
        epochs     = int(cmd.epochs)
        lr         = float(cmd.lr)
        seed       = int(cmd.seed)
        last_round = round_id

        # 2) 选本地子集（确定性：seed + client_id）
        random.seed(seed + client_id)
        idx = list(range(len(train_full)))
        random.shuffle(idx)
        idx = idx[:subset_sz]
        ds = Subset(train_full, idx)
        loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True, num_workers=2, drop_last=True)

        # 3) 本地训练
        opt = torch.optim.SGD(model.parameters(), lr=lr, momentum=0.9)
        crit = nn.CrossEntropyLoss()
        model.train()
        for _ in range(epochs):
            for x, y in loader:
                x, y = x.to(device), y.to(device)
                opt.zero_grad(set_to_none=True)
                loss = crit(model(x), y)
                loss.backward()
                opt.step()

        # 4) 参数序列化（float32 原始字节）→ 发送 ClientUpdate
        raw = params_to_f32_bytes(model)

        # 兼容不同绑定的 sequence<octet> 映射：
        # 1) 先尝试 bytes（许多绑定就是 bytes/bytearray）；2) 若报类型错误，退回 List[int]。:contentReference[oaicite:3]{index=3}
        try:
            upd = ClientUpdate(client_id=client_id,
                               round_id=round_id,
                               num_samples=len(ds),
                               data=raw)                # bytes
        except Exception:
            upd = ClientUpdate(client_id=client_id,
                               round_id=round_id,
                               num_samples=len(ds),
                               data=list(raw))          # List[uint8]

        w_upd.write(upd)
        print(f"[Client {client_id}] sent update: round={round_id}, n={len(ds)}, bytes={len(raw)}")

        # （可选）5) 等控制端广播新模型并应用
        # for mb in r_model.take_iter(timeout=50):
        #     if int(mb.round_id) == round_id:
        #         # 若需要：把 mb.data 转回 bytes / list 再还原到参数
        #         pass

if __name__ == "__main__":
    main()
