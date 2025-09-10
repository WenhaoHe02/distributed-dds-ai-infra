# train_ddp_mnist_subset.py
# -*- coding: utf-8 -*-
import os, time
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms

# ---- 环境参数（保持和原版一致）
RANK      = int(os.environ.get("RANK", "0"))
WORLD     = int(os.environ.get("WORLD_SIZE", "1"))
DATA_DIR  = os.environ.get("DATA_DIR", "./data")
SUBSET    = int(os.environ.get("SUBSET", "60000"))

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

def make_loaders(data_dir, batch_size, device, subset=None):
    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),   # MNIST标准归一化
    ])
    train_ds = datasets.MNIST(root=data_dir, train=True,  download=True, transform=tfm)
    val_ds   = datasets.MNIST(root=data_dir, train=False, download=True, transform=tfm)

    # ---- 如果 subset 有限制，则只取前 subset 个样本
    if subset is not None and subset < len(train_ds):
        train_ds = Subset(train_ds, list(range(subset)))

    pin = (device.type == "cuda")
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True,
                              pin_memory=pin, drop_last=True)
    val_loader   = DataLoader(val_ds,   batch_size=1024, shuffle=False,
                              num_workers=0, pin_memory=pin)
    return train_loader, val_loader

def evaluate(model, val_loader, device):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for xb, yb in val_loader:
            xb, yb = xb.to(device), yb.to(device)
            logits = model(xb)
            pred = logits.argmax(dim=1)
            correct += (pred == yb).sum().item()
            total += yb.size(0)
    acc = correct / total
    model.train()
    return correct, total, acc

def main():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = MNISTNet().to(device)
    opt = optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
    loss_fn = nn.CrossEntropyLoss()

    # ---- 支持 subset
    train_loader, val_loader = make_loaders(DATA_DIR, batch_size=256, device=device, subset=SUBSET)
    if RANK == 0:
        print(f"[train] using {len(train_loader.dataset)} training samples (subset limit: {SUBSET})")

    epochs = 3
    eval_every = 100
    global_step = 0

    for ep in range(epochs):
        for xb, yb in train_loader:
            xb, yb = xb.to(device), yb.to(device)

            opt.zero_grad()
            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()
            opt.step()

            if global_step % 100 == 0:
                print(f"[rank {RANK}] step {global_step} loss={loss.item():.4f}")

            if (global_step + 1) % eval_every == 0:
                correct, total, acc = evaluate(model, val_loader, device)
                if RANK == 0:
                    print(f"[VAL] step {global_step:05d} acc={acc*100:.2f}% ({correct}/{total})")

            global_step += 1

    if RANK == 0:
        print("[train] done.")

if __name__ == "__main__":
    main()
