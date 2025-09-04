#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MNIST local training with INT8 sparse (DGC-style) as **model delta**:
- Track round-start weights p_ref
- After each step, compute delta_since_ref = p_cur - p_ref
- Send the **increment since last sent**: delta_to_send = delta_since_ref - sent_accum
- Error feedback: delta_ef = delta_to_send + residual
- Top-k + int8 quantization per packet; update residual & sent_accum
- At round end (comm_every=0), send the remaining delta

Wire formats (little-endian):
S8 (sparse int8, delta):  'S','8','\x00', ver=1 | dim(i32) | k(i32) | scale(f32) | idx[i32*k] | qvals[i8*k]
Q8 (dense int8, weights): 'Q','8','\x00', ver=1 | chunk(i32) | total(i64) | nChunks(i32) | scales[f32*n] | qvals[i8*total]
FP32 (weights): raw float32 little-endian

stdout: one JSON line with at least {"num_samples": <int>, "stream": true/false}
"""

import argparse, json, math, os, struct
from typing import Optional, List, Tuple
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.utils import parameters_to_vector, vector_to_parameters
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms
from concurrent.futures import ThreadPoolExecutor

# ---------------- Model ----------------
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
        x = self.pool(x)
        x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x))
        return self.fc2(x)

# -------------- Utils --------------
EPS = 1e-12

def make_shard_indices(n_total:int, num_clients:int, client_id:int, seed:int,
                       mode:str="contig", limit:Optional[int]=None) -> List[int]:
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
    return torch.cat([p.detach().view(-1).float() for p in model.parameters()], dim=0).contiguous()

def load_init_model(path: str, model: nn.Module):
    if not path or not os.path.exists(path):
        print(f"[PY] init_model {path} not found, skip")
        return
    with open(path, "rb") as f:
        magic4 = f.read(4); f.seek(0)
        if magic4.startswith(b'Q8'):
            header = f.read(4+1+4+8+4)
            magic4, ver, chunk, total, nChunks = struct.unpack('<4sBiqi', header)
            scales = np.frombuffer(f.read(nChunks*4), dtype=np.float32)
            qvals  = np.frombuffer(f.read(total), dtype=np.int8)
            vec = np.empty(total, dtype=np.float32)
            for i in range(nChunks):
                s, e = i*chunk, min((i+1)*chunk, total)
                vec[s:e] = qvals[s:e].astype(np.float32) * scales[i]
        elif magic4.startswith(b'S8'):
            header = f.read(4+1+4+4+4)
            magic4, ver, dim, k, scale = struct.unpack('<4sBiif', header)
            idxs = np.frombuffer(f.read(k*4), dtype=np.int32)
            vals = np.frombuffer(f.read(k), dtype=np.int8).astype(np.float32) * scale
            vec = np.zeros(dim, dtype=np.float32); vec[idxs] = vals
        else:
            vec = np.frombuffer(f.read(), dtype='<f4')
    vector_to_parameters(torch.from_numpy(vec.copy()).float(), model.parameters())
    print(f"[PY] init_model loaded {vec.size} floats")

# -------------- Q8 / S8 packers --------------
def q8_dense_pack(vec_f32: np.ndarray, chunk: int, out_path: str) -> dict:
    total = int(vec_f32.size)
    chunk = max(1, int(chunk))
    n_chunks = (total + chunk - 1) // chunk
    scales = np.empty(n_chunks, dtype=np.float32)
    qvals  = np.empty(total,    dtype=np.int8)
    for i in range(n_chunks):
        s = i*chunk; e = min(s+chunk, total)
        block = vec_f32[s:e]
        maxabs = float(np.max(np.abs(block))) if block.size>0 else 0.0
        sc = (maxabs / 127.0) if maxabs>0 else 1e-8
        scales[i] = sc
        qvals[s:e] = np.clip(np.round(block / sc), -128, 127).astype(np.int8)
    with open(out_path, "wb") as f:
        f.write(b'Q8\x00'); f.write(struct.pack('<B', 1))
        f.write(struct.pack('<i', chunk))
        f.write(struct.pack('<q', total))
        f.write(struct.pack('<i', n_chunks))
        f.write(scales.tobytes(order='C'))
        f.write(qvals.tobytes(order='C'))
    return {
        "codec":"int8/dense","chunk":chunk,"fp32_elems":total,
        "bytes_fp32": total*4,
        "bytes_packed": (4+1)+4+8+4 + n_chunks*4 + total
    }

def _topk_mask(x: np.ndarray, k: int):
    if k <= 0 or k >= x.size:
        idx = np.arange(x.size, dtype=np.int32)
        mask = np.ones_like(x, dtype=bool)
        return idx.astype(np.int32), mask
    kth = np.argpartition(np.abs(x), -k)[-k:]
    ord_idx = kth[np.argsort(-np.abs(x[kth]))]
    mask = np.zeros_like(x, dtype=bool); mask[ord_idx] = True
    return ord_idx.astype(np.int32), mask

def s8_sparse_pack(vals_f32: np.ndarray, dim: int, idx: np.ndarray, out_path: str) -> dict:
    maxabs = float(np.max(np.abs(vals_f32))) if vals_f32.size > 0 else 0.0
    scale = (maxabs / 127.0) if maxabs>0 else 1e-8
    q = np.clip(np.round(vals_f32 / scale), -128, 127).astype(np.int8)
    with open(out_path, "wb") as f:
        f.write(b'S8\x00'); f.write(struct.pack('<B', 1))
        f.write(struct.pack('<i', dim))
        f.write(struct.pack('<i', int(idx.size)))
        f.write(struct.pack('<f', float(scale)))
        f.write(idx.astype('<i4').tobytes(order='C'))
        f.write(q.tobytes(order='C'))
    return {
        "codec":"int8_sparse","dim":dim,"k":int(idx.size),"scale":float(scale),
        "bytes_packed": (4+1)+4+4+4 + idx.size*4 + idx.size, "bytes_fp32": dim*4
    }

def s8_sparse_pack_parallel(vals_f32: np.ndarray, dim: int, idx: np.ndarray, out_path: str, n_threads: int = 4) -> dict:
    maxabs = float(np.max(np.abs(vals_f32))) if vals_f32.size > 0 else 0.0
    scale = (maxabs / 127.0) if maxabs>0 else 1e-8
    def quantize_block(block): return np.clip(np.round(block / scale), -128, 127).astype(np.int8)
    blocks = np.array_split(vals_f32, n_threads)
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        q_blocks = list(ex.map(quantize_block, blocks))
    q = np.concatenate(q_blocks)
    with open(out_path, "wb") as f:
        f.write(b'S8\x00'); f.write(struct.pack('<B', 1))
        f.write(struct.pack('<i', dim))
        f.write(struct.pack('<i', int(idx.size)))
        f.write(struct.pack('<f', float(scale)))
        f.write(idx.astype('<i4').tobytes(order='C'))
        f.write(q.tobytes(order='C'))
    return {
        "codec":"int8_sparse","dim":dim,"k":int(idx.size),"scale":float(scale),
        "bytes_packed": (4+1)+4+4+4 + idx.size*4 + idx.size, "bytes_fp32": dim*4
    }

# -------------- Δ 状态（误差反馈 + 分段发送累加） --------------
def delta_state_paths(state_dir: str):
    os.makedirs(state_dir, exist_ok=True)
    return (os.path.join(state_dir, "delta_residual.npy"),   # 误差残差
            os.path.join(state_dir, "delta_sent_accum.npy")) # 已发送的累计Δ

def delta_load_states(state_dir: str, dim: int):
    rp, sp = delta_state_paths(state_dir)
    r = np.load(rp) if os.path.exists(rp) else np.zeros(dim, dtype=np.float32)
    s = np.load(sp) if os.path.exists(sp) else np.zeros(dim, dtype=np.float32)
    if r.size != dim: r = np.zeros(dim, dtype=np.float32)
    if s.size != dim: s = np.zeros(dim, dtype=np.float32)
    return r, s

def delta_save_states(state_dir: str, residual: np.ndarray, sent_accum: np.ndarray):
    rp, sp = delta_state_paths(state_dir)
    np.save(rp, residual.astype(np.float32))
    np.save(sp, sent_accum.astype(np.float32))

# -------------- 训练与发送 --------------
def train_one_client(args):
    # 防呆：当 comm_every>0 但 out 是已存在的文件 → 降级为单包
    if args.compress == "int8_sparse" and args.comm_every > 0:
        try:
            if os.path.exists(args.out) and os.path.isfile(args.out):
                print("[PY][WARN] --comm_every>0 but --out is an existing FILE; fallback to single-file mode.")
                args.comm_every = 0
        except Exception:
            pass

    tfm = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)
    indices = make_shard_indices(len(train_full), args.num_clients, args.client_id, args.seed,
                                 mode=args.partition, limit=(args.subset if args.subset>0 else None))
    ds = Subset(train_full, indices)
    loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True, num_workers=0, drop_last=False)

    device = torch.device("cpu")
    model = Net().to(device)
    if args.init_model: load_init_model(args.init_model, model)

    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=0.9)
    criterion = nn.CrossEntropyLoss()

    # 参考权重（回合起点）
    p_ref = flatten_params(model).cpu().numpy()
    dim = int(p_ref.size)
    residual, sent_accum = delta_load_states(args.state_dir, dim)

    global_step = 0
    send_count = 0
    last_stats = None

    if args.compress == "int8_sparse" and args.comm_every > 0:
        os.makedirs(args.out, exist_ok=True)

    model.train()
    for _ in range(args.epochs):
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            opt.zero_grad(set_to_none=True)
            loss = criterion(model(x), y); loss.backward(); opt.step()
            global_step += 1

            # 每步/每N步发送“新增Δ”
            if args.compress == "int8_sparse" and args.comm_every > 0 and (global_step % args.comm_every) == 0:
                p_cur = flatten_params(model).cpu().numpy()
                delta_since_ref = (p_cur - p_ref)
                delta_to_send   = (delta_since_ref - sent_accum)
                delta_ef        = (delta_to_send + residual).astype(np.float32)

                # warmup 选择 k
                if args.dgc_warmup_rounds and args.round < args.dgc_warmup_rounds: warm = (args.round + 1)/float(args.dgc_warmup_rounds)
                else: warm = 1.0
                if args.sparse_k > 0: k = max(1, int(math.ceil(args.sparse_k * warm)))
                else: k = max(1, int(math.ceil(dim * max(0.0, args.sparse_ratio) * warm)))

                idx, _ = _topk_mask(delta_ef, k)
                vals   = delta_ef[idx].astype(np.float32)
                step_out = os.path.join(args.out, f"r{args.round:04d}_s{global_step:06d}.s8")
                last_stats = s8_sparse_pack_parallel(vals, dim, idx, step_out)

                # 回放 & 累计
                maxabs = float(np.max(np.abs(vals))) if vals.size>0 else 0.0
                scale  = (maxabs/127.0) if maxabs>0 else 1e-8
                q      = np.clip(np.round(vals/scale), -128, 127).astype(np.int8)
                sent   = np.zeros(dim, dtype=np.float32); sent[idx] = q.astype(np.float32) * scale

                residual   = (delta_ef - sent).astype(np.float32)
                sent_accum = (sent_accum + sent).astype(np.float32)
                delta_save_states(args.state_dir, residual, sent_accum)

                send_count += 1

    # 回合末处理
    if args.compress == "int8_sparse":
        if args.comm_every == 0:
            p_cur = flatten_params(model).cpu().numpy()
            delta_total = (p_cur - p_ref) - sent_accum
            delta_ef    = (delta_total + residual).astype(np.float32)

            if args.dgc_warmup_rounds and args.round < args.dgc_warmup_rounds: warm = (args.round + 1)/float(args.dgc_warmup_rounds)
            else: warm = 1.0
            if args.sparse_k > 0: k = max(1, int(math.ceil(args.sparse_k * warm)))
            else: k = max(1, int(math.ceil(dim * max(0.0, args.sparse_ratio) * warm)))

            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            idx, _ = _topk_mask(delta_ef, k)
            vals   = delta_ef[idx].astype(np.float32)
            stats  = s8_sparse_pack_parallel(vals, dim, idx, args.out)

            maxabs = float(np.max(np.abs(vals))) if vals.size>0 else 0.0
            scale  = (maxabs/127.0) if maxabs>0 else 1e-8
            q      = np.clip(np.round(vals/scale), -128, 127).astype(np.int8)
            sent   = np.zeros(dim, dtype=np.float32); sent[idx] = q.astype(np.float32) * scale

            residual   = (delta_ef - sent).astype(np.float32)
            sent_accum = (sent_accum + sent).astype(np.float32)
            delta_save_states(args.state_dir, residual, sent_accum)

            stats["stream"] = False
            return len(ds), stats
        else:
            # 多包已写入；若 send_count==0，发送一次兜底
            if send_count == 0:
                p_cur = flatten_params(model).cpu().numpy()
                delta_total = (p_cur - p_ref) - sent_accum
                delta_ef    = (delta_total + residual).astype(np.float32)

                if args.dgc_warmup_rounds and args.round < args.dgc_warmup_rounds: warm = (args.round + 1)/float(args.dgc_warmup_rounds)
                else: warm = 1.0
                if args.sparse_k > 0: k = max(1, int(math.ceil(args.sparse_k * warm)))
                else: k = max(1, int(math.ceil(dim * max(0.0, args.sparse_ratio) * warm)))

                os.makedirs(args.out, exist_ok=True)
                idx, _ = _topk_mask(delta_ef, k)
                vals   = delta_ef[idx].astype(np.float32)
                step_out = os.path.join(args.out, f"r{args.round:04d}_s{global_step:06d}.s8")
                last_stats = s8_sparse_pack_parallel(vals, dim, idx, step_out)

                maxabs = float(np.max(np.abs(vals))) if vals.size>0 else 0.0
                scale  = (maxabs/127.0) if maxabs>0 else 1e-8
                q      = np.clip(np.round(vals/scale), -128, 127).astype(np.int8)
                sent   = np.zeros(dim, dtype=np.float32); sent[idx] = q.astype(np.float32) * scale
                residual   = (delta_ef - sent).astype(np.float32)
                sent_accum = (sent_accum + sent).astype(np.float32)
                delta_save_states(args.state_dir, residual, sent_accum)

                send_count = 1

            return len(ds), {
                "codec":"int8_sparse_stream","packets": int(send_count),
                "bytes_packed_last": int(last_stats.get("bytes_packed",0)) if last_stats else 0,
                "bytes_fp32": int(last_stats.get("bytes_fp32", dim*4) if last_stats else dim*4),
                "stream": True
            }

    elif args.compress == "int8":
        vec = parameters_to_vector(model.parameters()).detach().float().contiguous().cpu().numpy()
        stats = q8_dense_pack(vec, chunk=args.chunk, out_path=args.out)
        stats["stream"] = False
        return len(ds), stats

    else:
        vec = parameters_to_vector(model.parameters()).detach().float().contiguous().cpu().numpy()
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "wb") as f:
            f.write(vec.astype("<f4").tobytes(order='C'))
        stats = {"codec":"fp32","fp32_elems": vec.size,"bytes_fp32": int(vec.size*4),
                 "bytes_packed": int(vec.size*4),"compression_ratio":1.0,"stream": False}
        return len(ds), stats

def main():
    ap = argparse.ArgumentParser(description="MNIST local training + DGC int8 sparse (delta) v3")
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

    ap.add_argument("--compress",    type=str, default="int8_sparse", choices=["int8_sparse","int8","fp32"])
    ap.add_argument("--chunk",       type=int, default=1024)
    ap.add_argument("--sparse_k",    type=int, default=0)
    ap.add_argument("--sparse_ratio",type=float, default=0.001)

    # DGC 占位参数（用于 warmup/兼容）
    ap.add_argument("--dgc_momentum",       type=float, default=0.9)
    ap.add_argument("--dgc_clip_norm",      type=float, default=0.0)
    ap.add_argument("--dgc_mask_momentum",  type=int,   default=1)
    ap.add_argument("--dgc_warmup_rounds",  type=int,   default=1)

    ap.add_argument("--round",              type=int,   default=0)
    ap.add_argument("--state_dir",          type=str,   default="state")

    ap.add_argument("--comm_every", type=int, default=1,
        help="0=send only at round end; 1=send every step (default); N=send every N steps")

    ap.add_argument("--init_model", type=str, default=None)
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
