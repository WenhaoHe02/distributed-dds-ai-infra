import time, argparse
import torch, torch.nn as nn, torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms

class Net(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.pool  = nn.MaxPool2d(2)
        self.fc1   = nn.Linear(64*12*12, 128)
        self.fc2   = nn.Linear(128, 10)
    def forward(self, x):
        x = F.relu(self.conv1(x))
        x = F.relu(self.conv2(x))
        x = self.pool(x)
        x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x))
        return self.fc2(x)

@torch.no_grad()
def evaluate(model, loader, device):
    model.eval()
    correct = 0
    total = 0
    for x, y in loader:
        x, y = x.to(device), y.to(device)
        pred = model(x).argmax(1)
        correct += (pred == y).sum().item()
        total += y.numel()
    return correct / total

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", type=str, default="./data")
    ap.add_argument("--epochs", type=int, default=5)
    ap.add_argument("--batch_size", type=int, default=64)
    ap.add_argument("--lr", type=float, default=0.01)
    ap.add_argument("--momentum", type=float, default=0.9)
    ap.add_argument("--subset", type=int, default=10000,
                    help="训练集中最多使用多少样本（默认30000用于匹配联邦版本总量）")
    args = ap.parse_args()

    device = torch.device("cpu")

    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),
    ])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)
    test_ds    = datasets.MNIST(args.data_dir, train=False, download=True, transform=tfm)

    train_ds = Subset(train_full, list(range(min(args.subset, len(train_full)))))
    print(f"[Centralized] using total {len(train_ds)} samples (to match FL baseline)")

    train_loader = DataLoader(train_ds, batch_size=args.batch_size, shuffle=True, num_workers=2, drop_last=True)
    test_loader  = DataLoader(test_ds, batch_size=64, shuffle=False, num_workers=2)

    model = Net().to(device)
    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=args.momentum)
    crit = nn.CrossEntropyLoss()

    print(f"[Centralized] epochs={args.epochs} batch={args.batch_size} lr={args.lr}")

    t0 = time.time()
    for ep in range(1, args.epochs + 1):
        model.train()
        loss_sum = 0.0
        for x, y in train_loader:
            x, y = x.to(device), y.to(device)
            opt.zero_grad(set_to_none=True)
            logits = model(x)
            loss = crit(logits, y)
            loss.backward()
            opt.step()
            loss_sum += loss.item()
        acc = evaluate(model, test_loader, device)
        print(f"Epoch {ep}: loss={loss_sum/len(train_loader):.4f}  acc={acc*100:.2f}%")
    print(f"[Centralized] total_time={time.time() - t0:.2f}s")

    torch.save(model.state_dict(), "mnist_centralized.pt")
    print("Saved: mnist_centralized.pt")

if __name__ == "__main__":
    main()
