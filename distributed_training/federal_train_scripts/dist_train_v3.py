#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, math, os, random
from typing import List, Optional

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.utils import parameters_to_vector, vector_to_parameters
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms


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
        x = self.pool(x); x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x)); return self.fc2(x)


# ------------- Data shard --------------
def make_shard_indices(n_total:int, num_clients:int, client_id:int, seed:int,
                       mode:str="contig", limit:Optional[int]=None) -> List[int]:
    g = torch.Generator().manual_seed(seed)
    perm = torch.randperm(n_total, generator=g).tolist()
    client_id = client_id % max(1, num_clients)
    if mode == "contig":
        shard = math.ceil(n_total / num_clients)
        start = client_id * shard
        idxs = perm[start:start+shard]
    elif mode == "stride":
        idxs = perm[client_id::num_clients]
    else:
        raise ValueError("partition must be contig|stride")
    if limit and limit > 0:
        idxs = idxs[:limit]
    return idxs


# ------------- IO helpers --------------
def save_dense_fp32(vec: np.ndarray, out_path:str):
    with open(out_path, "wb") as f: f.write(vec.astype("<f4").tobytes(order="C"))

def write_s4(dim:int, idx: np.ndarray, vals: np.ndarray, path: str):
    with open(path, "wb") as f:
        f.write(b"S4\x00"); f.write(bytes([1]))              # magic + ver=1
        f.write(np.int32(dim).tobytes()); f.write(np.int32(idx.size).tobytes())
        f.write(idx.astype("<i4").tobytes()); f.write(vals.astype("<f4").tobytes())

def load_init_model_fp32(path: Optional[str], model: nn.Module) -> Optional[torch.Tensor]:
    if not path or not os.path.exists(path): return None
    raw = np.fromfile(path, dtype="<f4"); vec = torch.from_numpy(raw.copy()).float()
    try:
        vector_to_parameters(vec.clone(), model.parameters())
        print(f"=> loaded init fp32: {path}, elems={vec.numel()}"); return vec
    except Exception as e:
        print(f"=> failed to load init: {e}"); return None


# ------------- Top-k helpers -----------
def topk_indices_vals(vec: torch.Tensor, ratio: float):
    dim = int(vec.numel())
    r = max(0.0, min(1.0, float(ratio)))
    k = max(1, int(dim * r))
    top_vals, _ = torch.topk(vec.abs(), k, largest=True, sorted=False)
    thr = top_vals.min()
    mask = vec.abs() >= thr
    idx  = mask.nonzero().view(-1)
    vals = vec[idx]
    return idx, vals


# ---------------- Main -----------------
def main():
    ap = argparse.ArgumentParser()
    # —— 来自 Java Client ——
    ap.add_argument("--client_id", type=int, required=True)
    ap.add_argument("--num_clients", type=int, required=True)
    ap.add_argument("--round", type=int, required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--epochs", type=int, required=True)
    ap.add_argument("--batch_size", type=int, default=32)
    ap.add_argument("--lr", type=float, required=True)
    ap.add_argument("--data_dir", type=str, required=True)
    ap.add_argument("--out", type=str, required=True)             # 流式=目录；非流式=文件
    # —— 压缩控制 ——
    ap.add_argument("--compress", type=str, choices=["fp32_full","fp32_sparse"], required=True)
    ap.add_argument("--sparse_ratio", type=float, default=0.1)    # 10%
    ap.add_argument("--comm_every", type=int, default=0)          # >0: 每N步；=0: 每epoch；<0: 单包
    ap.add_argument("--subset", type=int, default=None)           # Controller 下发子集大小
    # —— 保留参数（与旧版兼容） ——
    ap.add_argument("--partition", type=str, default="contig")
    ap.add_argument("--init_model", type=str, default=None)
    ap.add_argument("--state_dir", type=str, default=None)
    args = ap.parse_args()

    random.seed(args.seed); np.random.seed(args.seed); torch.manual_seed(args.seed)

    # 输出路径判定
    def decide_output(args):
        stream_mode = (args.compress == "fp32_sparse") and (int(args.comm_every) >= 0)
        if stream_mode:
            out_dir = args.out
            if os.path.exists(out_dir) and not os.path.isdir(out_dir):
                raise RuntimeError(f"--out 指向已存在的文件而非目录: {out_dir}")
            os.makedirs(out_dir, exist_ok=True)
            return True, out_dir, None
        else:
            out_bin = args.out
            parent = os.path.dirname(out_bin)
            if parent and not os.path.exists(parent):
                os.makedirs(parent, exist_ok=True)
            return False, None, out_bin

    stream_mode, out_dir, out_bin = decide_output(args)

    # Data
    tfm = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)
    limit = args.subset if args.subset and args.subset > 0 else None
    idxs = make_shard_indices(len(train_full), args.num_clients, args.client_id, args.seed, args.partition, limit=limit)
    ds = Subset(train_full, idxs)
    loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True, drop_last=False)

    # Model / Opt
    device = torch.device("cpu")
    model = Net().to(device)
    init_vec = load_init_model_fp32(args.init_model, model)  # 若没有则为 None
    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=0.9, nesterov=False)
    criterion = nn.CrossEntropyLoss()

    # comm_every 规范化
    steps_per_epoch = math.ceil(len(ds) / max(1, args.batch_size))
    if args.comm_every == 0:
        comm_every = steps_per_epoch      # 每epoch发一包（流式）
        stream_mode = (args.compress == "fp32_sparse")
    elif args.comm_every > 0:
        comm_every = args.comm_every      # 每N步发一包（流式）
        stream_mode = (args.compress == "fp32_sparse")
    else:
        comm_every = -1                   # 整轮单包（非流式）
        stream_mode = False

    # 窗口基线快照
    w_snap0 = parameters_to_vector(model.parameters()).detach().clone()
    # 首轮 + 稀疏 + 无 init：把基线改为 0（Δ=权重）
    if args.compress == "fp32_sparse" and int(args.round) <= 1 and (args.init_model in (None, "", "None")):
        w_snap0.zero_()
    w_snap = w_snap0.clone()

    pkt_id = 0
    step_in_win = 0

    model.train()
    for ep in range(args.epochs):
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            opt.zero_grad(set_to_none=True)
            loss = criterion(model(x), y)
            loss.backward()
            opt.step()

            step_in_win += 1
            if stream_mode and step_in_win >= comm_every:
                # 计算窗口 Δ，并发包
                w_now = parameters_to_vector(model.parameters()).detach()
                delta = (w_now - w_snap).detach()
                if args.compress == "fp32_sparse":
                    idx, vals = topk_indices_vals(delta, args.sparse_ratio)
                    pkt_id += 1
                    fname = os.path.join(out_dir, f"{pkt_id:05d}.s4")
                    write_s4(delta.numel(), idx.cpu().numpy().astype(np.int32), vals.cpu().numpy().astype(np.float32), fname)
                else:
                    pkt_id += 1
                    fname = os.path.join(out_dir, f"{pkt_id:05d}.bin")
                    vec = w_now.detach().float().cpu().numpy()
                    save_dense_fp32(vec, fname)
                # 滚动窗口
                w_snap = w_now.clone()
                step_in_win = 0

        # epoch 结束，若采用每epoch发包，已在上面触发

    # 处理尾包
    if stream_mode and step_in_win > 0:
        w_now = parameters_to_vector(model.parameters()).detach()
        delta = (w_now - w_snap).detach()
        if args.compress == "fp32_sparse":
            idx, vals = topk_indices_vals(delta, args.sparse_ratio)
            if idx.numel() > 0:
                pkt_id += 1
                fname = os.path.join(out_dir, f"{pkt_id:05d}.s4")
                write_s4(delta.numel(), idx.cpu().numpy().astype(np.int32), vals.cpu().numpy().astype(np.float32), fname)
        else:
            pkt_id += 1
            fname = os.path.join(out_dir, f"{pkt_id:05d}.bin")
            vec = w_now.detach().float().cpu().numpy()
            save_dense_fp32(vec, fname)

    # 非流式：整轮单包
    if not stream_mode:
        if args.compress == "fp32_full":
            vec = parameters_to_vector(model.parameters()).detach().float().cpu().numpy()
            save_dense_fp32(vec, args.out)
        else:
            # 单包稀疏：总 Δ = w_final - w_snap0
            w_final = parameters_to_vector(model.parameters()).detach()
            delta = (w_final - w_snap0).detach()
            idx, vals = topk_indices_vals(delta, args.sparse_ratio)
            write_s4(delta.numel(), idx.cpu().numpy().astype(np.int32), vals.cpu().numpy().astype(np.float32), args.out)

    # 输出 JSON（Client 用于读取 num_samples）
    meta = {"codec": ("fp32_dense" if args.compress == "fp32_full" else "fp32_sparse"),
            "num_samples": len(ds)}
    print(json.dumps(meta, ensure_ascii=False))


if __name__ == "__main__":
    main()
