#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MNIST local training with INT8 sparse compression (DGC-style):
- Error feedback (residual accumulation)
- Momentum correction (send velocity), momentum factor masking
- Warm-up schedule for sparsity
- Optional local gradient clipping
Also supports dense INT8 (Q8) and raw FP32 fallback.

Outputs:
  1) Writes binary update to --out (S8 sparse / Q8 dense / FP32 bytes)
  2) Prints a single JSON line to stdout with {"num_samples": ..., ...}
     so Java can parse num_samples; other fields are informative.

Binary wire formats (little-endian):

S8 SPARSE (default when --compress int8_sparse):
  magic:  'S','8',0, ver(uint8)=1
  dim(int32)                 # total number of elements
  k(int32)                   # number of sparse entries
  scale(float32)             # single global scale for int8 values
  indices[int32 * k]
  values[int8  * k]

Q8 DENSE (when --compress int8):
  magic:  'Q','8',0, ver(uint8)=1
  chunk(int32)               # chunk size for per-chunk scale
  total_len(int64)           # total number of elements
  nChunks(int32)
  scales[float32 * nChunks]
  qvalues[int8 * total_len]

FP32 DENSE (when --compress fp32 or empty):
  raw float32 little-endian for the whole vector
"""

import argparse, json, math, os, struct, sys
from typing import Optional, List, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.utils import parameters_to_vector, vector_to_parameters
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms
from concurrent.futures import ThreadPoolExecutor

def load_init_model(path: str, model: nn.Module):
    """Load model parameters from bin file (fp32, Q8 dense, or S8 sparse)."""
    if not os.path.exists(path):
        print(f"[PY] init_model {path} not found, skipping")
        return

    with open(path, "rb") as f:
        magic = f.read(3)   # first 3 bytes
        f.seek(0)

        if magic.startswith(b'Q8'):
            # dense int8
            header = f.read(4+1+4+8+4)
            _, ver, chunk, total, nChunks = struct.unpack('<3sBiqi', header)
            scales = np.frombuffer(f.read(nChunks*4), dtype=np.float32)
            qvals  = np.frombuffer(f.read(total), dtype=np.int8)
            vec = np.empty(total, dtype=np.float32)
            for i in range(nChunks):
                s, e = i*chunk, min((i+1)*chunk, total)
                vec[s:e] = qvals[s:e].astype(np.float32) * scales[i]
        elif magic.startswith(b'S8'):
            # sparse int8
            header = f.read(4+1+4+4+4)
            _, ver, dim, k, scale = struct.unpack('<3sBii f', header)
            idxs = np.frombuffer(f.read(k*4), dtype=np.int32)
            vals = np.frombuffer(f.read(k), dtype=np.int8).astype(np.float32) * scale
            vec = np.zeros(dim, dtype=np.float32)
            vec[idxs] = vals
        else:
            # assume raw fp32
            vec = np.frombuffer(f.read(), dtype='<f4')

    # assign into model
    try:
        vector_to_parameters(torch.from_numpy(vec.copy()).float(), model.parameters())
        print(f"[PY] init_model loaded from {path}, {vec.size} floats")
    except Exception as e:
        print(f"[PY] Failed to load init_model: {e}")


# ------------------------------
# Model (same as earlier Net)
# ------------------------------
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


# ------------------------------
# Utilities
# ------------------------------
def make_shard_indices(n_total:int, num_clients:int, client_id:int,
                       seed:int, mode:str="contig", limit:Optional[int]=None) -> List[int]:
    """Return this client's deterministic sample indices."""
    g = torch.Generator().manual_seed(seed)
    perm = torch.randperm(n_total, generator=g).tolist()

    client_id = client_id % max(1, num_clients)
    if mode == "contig":
        shard = math.ceil(n_total / num_clients)
        start = client_id * shard
        idxs = perm[start:start + shard]
    elif mode == "stride":
        idxs = perm[client_id::num_clients]
    else:
        raise ValueError("partition mode must be 'contig' or 'stride'")

    if limit is not None and limit > 0:
        idxs = idxs[:limit]
    return idxs


def flatten_params(model: nn.Module) -> torch.Tensor:
    return parameters_to_vector(m.detach().float() for m in model.parameters())


def flatten_grads(model: nn.Module) -> torch.Tensor:
    vecs = []
    for p in model.parameters():
        if p.grad is None:
            vecs.append(torch.zeros_like(p.detach()).view(-1))
        else:
            vecs.append(p.grad.detach().float().view(-1))
    return torch.cat(vecs, dim=0).contiguous()


def average_full_batch_grads(model: nn.Module, loader: DataLoader, criterion: nn.Module) -> torch.Tensor:
    """
    Compute average gradient over the whole subset (not just last mini-batch).
    """
    model.zero_grad(set_to_none=True)
    device = next(model.parameters()).device
    num_batches = 0
    for x, y in loader:
        x, y = x.to(device), y.to(device)
        out = model(x)
        loss = criterion(out, y)
        loss.backward()
        num_batches += 1
    # divide all grads by num_batches
    with torch.no_grad():
        for p in model.parameters():
            if p.grad is not None:
                p.grad.div_(max(1, num_batches))
    return flatten_grads(model)


# ------------------------------
# INT8 dense (Q8) codec
# ------------------------------
def q8_dense_pack(vec_f32: np.ndarray, chunk: int, out_path: str) -> dict:
    """
    Pack an FP32 vector as dense INT8 with per-chunk scales.
    Header:
      'Q','8',0, ver=1 | chunk(i32) | total_len(i64) | nChunks(i32) | scales[f32*n] | int8_vals[total_len]
    """
    total = int(vec_f32.size)
    chunk = max(1, int(chunk))
    n_chunks = (total + chunk - 1) // chunk

    # compute scales
    scales = np.empty(n_chunks, dtype=np.float32)
    qvals  = np.empty(total,    dtype=np.int8)

    for i in range(n_chunks):
        s = i * chunk
        e = min(s + chunk, total)
        block = vec_f32[s:e]
        maxabs = float(np.max(np.abs(block))) if block.size > 0 else 0.0
        sc = (maxabs / 127.0) if maxabs > 0 else 1e-8
        scales[i] = sc
        q = np.clip(np.round(block / sc), -128, 127).astype(np.int8)
        qvals[s:e] = q

    with open(out_path, "wb") as f:
        f.write(b'Q8\x00')
        f.write(struct.pack('<B', 1))       # ver
        f.write(struct.pack('<i', chunk))
        f.write(struct.pack('<q', total))
        f.write(struct.pack('<i', n_chunks))
        f.write(scales.tobytes(order='C'))
        f.write(qvals.tobytes(order='C'))

    bytes_packed = (4+1)+4+8+4 + n_chunks*4 + total  # header + scales + int8
    return {
        "codec": "int8/dense",
        "chunk": chunk,
        "fp32_elems": total,
        "bytes_fp32": total*4,
        "bytes_packed": bytes_packed,
        "compression_ratio": (total*4)/bytes_packed if bytes_packed > 0 else 0.0
    }


# ------------------------------
# INT8 sparse (S8) codec with DGC blocks
# ------------------------------
EPS = 1e-12

def _topk_mask(x: np.ndarray, k: int) -> Tuple[np.ndarray, np.ndarray]:
    if k <= 0 or k >= x.size:
        idx = np.arange(x.size, dtype=np.int32)
        mask = np.ones_like(x, dtype=bool)
        return idx, mask
    kth = np.argpartition(np.abs(x), -k)[-k:]
    ord_idx = kth[np.argsort(-np.abs(x[kth]))]  # sort descending by magnitude
    mask = np.zeros_like(x, dtype=bool)
    mask[ord_idx] = True
    return ord_idx.astype(np.int32), mask


def s8_sparse_pack(vals_f32: np.ndarray, dim: int, idx: np.ndarray, out_path: str) -> dict:
    # one global scale
    maxabs = float(np.max(np.abs(vals_f32))) if vals_f32.size > 0 else 0.0
    scale = (maxabs / 127.0) if maxabs > 0 else 1e-8
    q = np.clip(np.round(vals_f32 / scale), -128, 127).astype(np.int8)

    with open(out_path, "wb") as f:
        f.write(b'S8\x00')
        f.write(struct.pack('<B', 1))      # ver
        f.write(struct.pack('<i', dim))
        f.write(struct.pack('<i', int(idx.size)))
        f.write(struct.pack('<f', float(scale)))
        f.write(idx.astype('<i4').tobytes(order='C'))
        f.write(q.tobytes(order='C'))

    return {
        "codec": "int8_sparse",
        "dim": dim,
        "k": int(idx.size),
        "scale": float(scale),
        "bytes_packed": (4+1)+4+4+4 + idx.size*4 + idx.size,  # header + indices + int8
        "bytes_fp32": dim*4
    }

def s8_sparse_pack_parallel(vals_f32: np.ndarray, dim: int, idx: np.ndarray, out_path: str, n_threads: int = 4) -> dict:

    maxabs = float(np.max(np.abs(vals_f32))) if vals_f32.size > 0 else 0.0
    scale = (maxabs / 127.0) if maxabs > 0 else 1e-8


    def quantize_block(block):
        return np.clip(np.round(block / scale), -128, 127).astype(np.int8)


    blocks = np.array_split(vals_f32, n_threads)
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        q_blocks = list(executor.map(quantize_block, blocks))
    q = np.concatenate(q_blocks)

    # 4. 写入文件
    with open(out_path, "wb") as f:
        f.write(b'S8\x00')
        f.write(struct.pack('<B', 1))      # version
        f.write(struct.pack('<i', dim))
        f.write(struct.pack('<i', int(idx.size)))
        f.write(struct.pack('<f', float(scale)))
        f.write(idx.astype('<i4').tobytes(order='C'))
        f.write(q.tobytes(order='C'))

    return {
        "codec": "int8_sparse",
        "dim": dim,
        "k": int(idx.size),
        "scale": float(scale),
        "bytes_packed": (4+1)+4+4+4 + idx.size*4 + idx.size,
        "bytes_fp32": dim*4
    }


def dgc_build_and_pack(model: nn.Module,
                       loader: DataLoader,
                       out_path: str,
                       state_dir: str,
                       sparse_k: int = 0,
                       sparse_ratio: float = 0.001,
                       momentum: float = 0.9,
                       clip_norm: float = 0.0,
                       mask_momentum: bool = True,
                       round_id: int = 0,
                       warmup_rounds: int = 0) -> dict:
    """
    DGC-style:
      1) g = average gradient over whole subset
      2) g += residual (error feedback)
      3) local grad clipping (optional)
      4) v = m * v_prev + g  (momentum correction)
      5) warm-up schedule to pick sparsity
      6) top-k indices on |v|
      7) pack v[k] as S8 sparse
      8) residual_next = v - dequant(v[k])  ;  momentum factor masking: keep momentum only on sent indices
    Persistent states: residual.npy, momentum.npy (saved under state_dir)
    """
    os.makedirs(state_dir, exist_ok=True)
    res_path = os.path.join(state_dir, "residual.npy")
    mom_path = os.path.join(state_dir, "momentum.npy")

    criterion = nn.CrossEntropyLoss()
    device = next(model.parameters()).device
    model.train(False)  # we only compute grads next; BN/Dropout off

    # compute average gradient over the subset
    g_torch = average_full_batch_grads(model, loader, criterion)
    g = g_torch.detach().cpu().float().numpy()
    dim = g.size

    residual_prev = np.load(res_path) if os.path.exists(res_path) else np.zeros(dim, dtype=np.float32)
    v_prev        = np.load(mom_path) if os.path.exists(mom_path) else np.zeros(dim, dtype=np.float32)

    # error feedback
    g = g + residual_prev

    # local gradient clipping
    if clip_norm and clip_norm > 0.0:
        nrm = float(np.linalg.norm(g))
        if nrm > clip_norm:
            g = (g * (clip_norm / (nrm + EPS))).astype(np.float32)

    # momentum correction: send velocity
    v = (momentum * v_prev + g).astype(np.float32)

    # warm-up on sparsity (increase k from small to target)
    if warmup_rounds and warmup_rounds > 0 and round_id < warmup_rounds:
        warm_scale = (round_id + 1) / float(warmup_rounds)  # (0,1]
    else:
        warm_scale = 1.0

    if sparse_k and sparse_k > 0:
        k = max(1, int(math.ceil(sparse_k * warm_scale)))
    else:
        k = max(1, int(math.ceil(dim * max(0.0, sparse_ratio) * warm_scale)))

    idx, mask = _topk_mask(v, k)
    vals = v[idx].astype(np.float32)

    # pack as S8 sparse
    stats = s8_sparse_pack(vals, dim, idx, out_path)
    #stats = s8_sparse_pack_parallel(vals, dim, idx, out_path)

    # reconstruct sent part to compute new residual
    maxabs = float(np.max(np.abs(vals))) if vals.size > 0 else 0.0
    scale = (maxabs / 127.0) if maxabs > 0 else 1e-8
    q = np.clip(np.round(vals / scale), -128, 127).astype(np.int8)
    v_sent = np.zeros_like(v, dtype=np.float32)
    v_sent[idx] = (q.astype(np.float32) * scale)

    residual_next = (v - v_sent).astype(np.float32)

    # momentum factor masking: keep momentum only on sent indices
    if mask_momentum:
        v_next = np.zeros_like(v, dtype=np.float32)
        v_next[idx] = v[idx]
    else:
        v_next = v

    np.save(res_path, residual_next)
    np.save(mom_path, v_next)

    stats.update({
        "k": k,
        "scale": float(scale),
        "bytes_fp32": dim * 4,
        "compression_ratio": (dim*4) / float(stats["bytes_packed"]) if stats["bytes_packed"] > 0 else 0.0
    })
    return stats


# ------------------------------
# Training
# ------------------------------
def train_one_client(args) -> Tuple[int, dict]:
    # dataset
    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)

    indices = make_shard_indices(
        n_total=len(train_full),
        num_clients=args.num_clients,
        client_id=args.client_id,
        seed=args.seed,
        mode=args.partition,
        limit=args.subset if args.subset and args.subset > 0 else None
    )
    ds = Subset(train_full, indices)
    loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True, num_workers=0, drop_last=False)

    # model / optimizer
    device = torch.device("cpu")
    model = Net().to(device)

    # 如果提供了 init_model 就加载
    if args.init_model:
        load_init_model(args.init_model, model)

    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=0.9)

    # federal_train_scripts
    model.train()
    criterion = nn.CrossEntropyLoss()
    for _ in range(args.epochs):
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            opt.zero_grad(set_to_none=True)
            loss = criterion(model(x), y)
            loss.backward()
            opt.step()

    # choose what to pack:
    # - "int8_sparse": DGC-style on average gradient + states; writes to args.out
    # - "int8": dense Q8 on full parameters
    # - "fp32": raw fp32 on full parameters
    state_dir = args.state_dir if args.state_dir else os.path.join(args.data_dir, f"client_{args.client_id}_state")
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    if args.compress == "int8_sparse":
        stats = dgc_build_and_pack(
            model, loader, args.out, state_dir,
            sparse_k=args.sparse_k,
            sparse_ratio=args.sparse_ratio,
            momentum=args.dgc_momentum,
            clip_norm=args.dgc_clip_norm,
            mask_momentum=bool(args.dgc_mask_momentum),
            round_id=args.round,
            warmup_rounds=args.dgc_warmup_rounds
        )
    elif args.compress == "int8":
        vec = parameters_to_vector(model.parameters()).detach().float().contiguous().view(-1).cpu().numpy()
        stats = q8_dense_pack(vec, chunk=args.chunk, out_path=args.out)
    else:
        vec = parameters_to_vector(model.parameters()).detach().float().contiguous().view(-1).cpu().numpy()
        with open(args.out, "wb") as f:
            f.write(vec.astype("<f4").tobytes(order='C'))
        stats = {
            "codec": "fp32",
            "fp32_elems": vec.size,
            "bytes_fp32": int(vec.size * 4),
            "bytes_packed": int(vec.size * 4),
            "compression_ratio": 1.0
        }

    return len(ds), stats


def main():
    ap = argparse.ArgumentParser(description="MNIST local training + INT8 sparse packing (DGC).")
    ap.add_argument("--client_id",   type=int, required=True)
    ap.add_argument("--num_clients", type=int, required=True)
    ap.add_argument("--seed",        type=int, required=True)
    ap.add_argument("--subset",      type=int, default=0)
    ap.add_argument("--epochs",      type=int, required=True)
    ap.add_argument("--lr",          type=float, required=True)
    ap.add_argument("--batch_size",  type=int, default=32)
    ap.add_argument("--data_dir",    type=str, required=True)
    ap.add_argument("--out",         type=str, required=True)
    ap.add_argument("--partition",   type=str, default="contig", choices=["contig","stride"])

    # compression switches
    ap.add_argument("--compress",    type=str, default="int8_sparse", choices=["int8_sparse","int8","fp32"])
    ap.add_argument("--chunk",       type=int, default=1024)     # for dense int8
    ap.add_argument("--sparse_k",    type=int, default=0)        # top-k; if 0 use ratio
    ap.add_argument("--sparse_ratio",type=float, default=0.001)  # 0.1%

    # DGC knobs
    ap.add_argument("--dgc_momentum",       type=float, default=0.9)
    ap.add_argument("--dgc_clip_norm",      type=float, default=0.0)
    ap.add_argument("--dgc_mask_momentum",  type=int,   default=1)  # 1=true
    ap.add_argument("--dgc_warmup_rounds",  type=int,   default=0)
    ap.add_argument("--round",              type=int,   default=0)  # pass from Java for warmup schedule
    ap.add_argument("--state_dir",          type=str,   default="D:/Study/SummerSchool/codes/distributed-dds-ai-serving-system/distributed_training/federal_train_scripts/log") # residual/momentum persistence

    ap.add_argument("--init_model", type=str, default=None,
                    help="Path to initial model weights (fp32/Q8/S8)")

    args = ap.parse_args()

    num_samples, stats = train_one_client(args)
    meta = {
        "client_id": args.client_id,
        "num_clients": args.num_clients,
        "num_samples": int(num_samples),
        **stats
    }
    print(json.dumps(meta, ensure_ascii=False))


if __name__ == "__main__":
    main()
