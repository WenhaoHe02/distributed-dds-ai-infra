import argparse, json, math, struct
from typing import Optional, Tuple

import numpy as np
import torch, torch.nn as nn, torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms
from torch.nn.utils import parameters_to_vector

# ------------------ Model ------------------
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

# ------------------ Utils ------------------
def params_f32_vector(model: nn.Module) -> torch.Tensor:
    """Flatten model params as float32 tensor on CPU."""
    vec = parameters_to_vector(model.parameters()).detach().to(torch.float32).contiguous().cpu().view(-1)
    return vec

def params_to_f32_bytes(model: nn.Module) -> bytes:
    """Legacy fp32 writer (little-endian)."""
    return params_f32_vector(model).numpy().tobytes()

# ========== INT8 per-chunk quantization ==========
# 二进制布局（全部小端）：
#   magic[3] = b'Q8\x00'
#   ver u8   = 1
#   chunk_size u32
#   total_elems u64   # 原始 float32 向量长度
#   num_chunks u32
#   scales[num_chunks] float32
#   data[total_elems] int8
MAGIC = b"Q8\x00"
VER   = 1

def quantize_int8_per_chunk(vec: torch.Tensor, chunk_size: int = 1024, eps: float = 1e-8) -> Tuple[np.ndarray, np.ndarray, int]:
    """Return (scales_f32, int8_data, num_chunks)."""
    assert vec.dtype == torch.float32 and vec.dim() == 1
    n = vec.numel()
    if n == 0:
        return np.zeros((0,), dtype=np.float32), np.zeros((0,), dtype=np.int8), 0
    num_chunks = (n + chunk_size - 1) // chunk_size
    scales = np.empty((num_chunks,), dtype=np.float32)
    q_all  = np.empty((n,), dtype=np.int8)

    v = vec.numpy()  # view, not a copy (contiguous ensured)
    for ci in range(num_chunks):
        s = ci * chunk_size
        e = min(s + chunk_size, n)
        chunk = v[s:e]
        amax = float(np.max(np.abs(chunk))) if e > s else 0.0
        scale = amax / 127.0 if amax > eps else 1.0  # 纯 0 块给 1.0，量化后全 0
        scales[ci] = scale
        if amax <= eps:
            q_all[s:e] = 0
        else:
            q = np.round(chunk / scale)
            q = np.clip(q, -127, 127).astype(np.int8, copy=False)
            q_all[s:e] = q
    return scales, q_all, num_chunks

def pack_qint8_blob(vec_f32: torch.Tensor, chunk_size: int = 1024) -> bytes:
    """Pack to bytes with header + scales + int8 payload (see layout above)."""
    scales, q, num_chunks = quantize_int8_per_chunk(vec_f32, chunk_size)
    n = vec_f32.numel()

    header = struct.pack(
        "<3sB I Q I",  # magic,u8, chunk,u32, total,u64, n_chunks,u32
        MAGIC, VER, chunk_size, n, num_chunks
    )
    body = scales.tobytes(order="C") + q.tobytes(order="C")
    return header + body

def unpack_qint8_blob(blob: bytes) -> np.ndarray:
    """给聚合端用：反量化成 float32 向量（numpy 数组）。"""
    off = 0
    magic, ver, chunk, total, n_chunks = struct.unpack_from("<3sB I Q I", blob, off)
    off += struct.calcsize("<3sB I Q I")
    if magic != MAGIC or ver != VER:
        raise ValueError("bad INT8 blob header")
    scales = np.frombuffer(blob, dtype=np.float32, count=n_chunks, offset=off)
    off += n_chunks * 4
    q = np.frombuffer(blob, dtype=np.int8, count=total, offset=off)
    out = np.empty((total,), dtype=np.float32)
    # 逐块反量化
    for ci in range(n_chunks):
        s = ci * chunk
        e = min(s + chunk, total)
        out[s:e] = q[s:e].astype(np.float32) * float(scales[ci])
    return out  # float32 np.ndarray

# ------------------ Data split ------------------
def make_shard_indices(n_total:int, num_clients:int, client_id:int,
                       seed:int, mode:str="contig", limit:Optional[int]=None):
    g = torch.Generator().manual_seed(seed)
    perm = torch.randperm(n_total, generator=g).tolist()
    client_id = client_id % max(1, num_clients)
    if mode == "contig":
        shard = math.ceil(n_total / max(1, num_clients))
        start = client_id * shard
        idxs = perm[start:start + shard]
    elif mode == "stride":
        idxs = perm[client_id::max(1, num_clients)]
    else:
        raise ValueError("mode must be 'contig' or 'stride'")
    if limit is not None:
        idxs = idxs[:limit]
    return idxs

# ------------------ Main ------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client_id",   type=int, required=True)
    ap.add_argument("--num_clients", type=int, default=1)   # 放宽：默认 1，避免调用端缺参直接崩
    ap.add_argument("--seed",        type=int, required=True)
    ap.add_argument("--subset",      type=int, default=None)
    ap.add_argument("--epochs",      type=int, required=True)
    ap.add_argument("--lr",          type=float, required=True)
    ap.add_argument("--batch_size",  type=int, default=128)
    ap.add_argument("--data_dir",    type=str, default="./data")
    ap.add_argument("--out",         type=str, required=True)
    ap.add_argument("--partition",   type=str, default="contig", choices=["contig","stride"])
    # 新增：压缩方式
    ap.add_argument("--compress",    type=str, default="int8", choices=["fp32","int8"])
    ap.add_argument("--chunk",       type=int, default=1024, help="INT8 分块大小")
    args = ap.parse_args()

    # MNIST
    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)

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
                        num_workers=0, drop_last=True)  # CPU 上先稳妥：worker=0，避免多进程开销
    model = Net()
    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=0.9)
    crit = nn.CrossEntropyLoss()

    torch.set_num_threads(max(1, torch.get_num_threads()))  # 交给 PyTorch 默认线程，或自行设置为物理核数

    model.train()
    for _ in range(args.epochs):
        for x, y in loader:
            opt.zero_grad(set_to_none=True)
            loss = crit(model(x), y)
            loss.backward()
            opt.step()

    # ------ 序列化 ------
    vec = params_f32_vector(model)
    if args.compress == "fp32":
        raw = vec.numpy().tobytes()
        codec = "fp32/raw"
        packed_bytes = len(raw)
        raw_fp32_bytes = vec.numel() * 4
    else:
        blob = pack_qint8_blob(vec, chunk_size=args.chunk)
        raw = blob
        codec = "int8/chunk"
        packed_bytes = len(blob)
        raw_fp32_bytes = vec.numel() * 4

    with open(args.out, "wb") as f:
        f.write(raw)

    print(json.dumps({
        "client_id": args.client_id,
        "num_clients": args.num_clients,
        "num_samples": len(ds),           # 注意：drop_last 后实际用到的样本数可能略少
        "partition": args.partition,
        "batch_size": args.batch_size,
        "epochs": args.epochs,
        "codec": codec,
        "chunk": args.chunk if args.compress == "int8" else None,
        "fp32_elems": vec.numel(),
        "bytes_fp32": raw_fp32_bytes,
        "bytes_packed": packed_bytes,
        "compression_ratio": round(raw_fp32_bytes / max(1, packed_bytes), 3)
    }))

if __name__ == "__main__":
    main()
