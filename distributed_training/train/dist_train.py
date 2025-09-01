import argparse, json, math
from typing import Optional

import torch, torch.nn as nn, torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms
from torch.nn.utils import parameters_to_vector

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
    vec = parameters_to_vector(model.parameters()).detach().float().contiguous().view(-1)
    return vec.numpy().tobytes()  # little-endian

def make_shard_indices(n_total:int, num_clients:int, client_id:int,
                       seed:int, mode:str="contig", limit:Optional[int]=None):
    """返回当前 client 的样本下标列表（无重叠、可复现）"""
    g = torch.Generator().manual_seed(seed)         # 仅用于打乱，保证复现
    perm = torch.randperm(n_total, generator=g).tolist()

    client_id = client_id % num_clients             # 容错：任何 id 都映射到 0..num_clients-1
    if mode == "contig":
        shard = math.ceil(n_total / num_clients)
        start = client_id * shard
        idxs = perm[start:start + shard]
    elif mode == "stride":
        idxs = perm[client_id::num_clients]
    else:
        raise ValueError("mode must be 'contig' or 'stride'")

    if limit is not None:
        idxs = idxs[:limit]
    return idxs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client_id",   type=int, required=True, help="从 0 开始的客户端编号")
    ap.add_argument("--num_clients", type=int, required=True, default=2, help="客户端总数")
    ap.add_argument("--seed",        type=int, required=True)
    ap.add_argument("--subset",      type=int, default=None, help="每个 client 取样最大数量（可选）")
    ap.add_argument("--epochs",      type=int, required=True)
    ap.add_argument("--lr",          type=float, required=True)
    ap.add_argument("--batch_size",  type=int, default=32)
    ap.add_argument("--data_dir",    type=str, default="./data")
    ap.add_argument("--out",         type=str, required=True)
    ap.add_argument("--partition",   type=str, default="contig", choices=["contig","stride"])
    args = ap.parse_args()

    # 本地 MNIST：看到 raw 文件就会用它们并生成 processed，不会真的联网下载
    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    train_full = datasets.MNIST(args.data_dir, train=True,  download=True, transform=tfm)
    # 如果你也需要测试集，可再建一个 test_full = datasets.MNIST(..., train=False, download=True)

    # 依据 client_id 切分
    shard_indices = make_shard_indices(
        n_total=len(train_full),
        num_clients=args.num_clients,
        client_id=args.client_id,
        seed=args.seed,
        mode=args.partition,
        limit=args.subset
    )
    ds = Subset(train_full, shard_indices)

    loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True,
                        num_workers=4, drop_last=True)
    model = Net()
    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=0.9)
    crit = nn.CrossEntropyLoss()

    model.train()
    for _ in range(args.epochs):
        for x, y in loader:
            opt.zero_grad(set_to_none=True)
            loss = crit(model(x), y)
            loss.backward()
            opt.step()

    raw = params_to_f32_bytes(model)
    with open(args.out, "wb") as f:
        f.write(raw)

    print(json.dumps({
        "client_id": args.client_id,
        "num_clients": args.num_clients,
        "num_samples": len(ds),
        "partition": args.partition,
        "bytes": len(raw)
    }))

if __name__ == "__main__":
    main()
