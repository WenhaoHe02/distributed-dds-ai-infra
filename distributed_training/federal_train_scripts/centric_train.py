import time, argparse
import torch, torch.nn as nn, torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms
import random

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
    ap.add_argument("--rounds", type=int, default=10, help="总共多少个 round")
    ap.add_argument("--epochs", type=int, default=1, help="每个 round 内训练多少个 epoch")
    ap.add_argument("--batch_size", type=int, default=64)
    ap.add_argument("--lr", type=float, default=0.01)
    ap.add_argument("--momentum", type=float, default=0.9)
    ap.add_argument("--subset", type=int, default=12000, help="每个 round 的样本数量")
    args = ap.parse_args()

    device = torch.device("cpu")

    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,)),
    ])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)
    test_ds    = datasets.MNIST(args.data_dir, train=False, download=True, transform=tfm)
    test_loader  = DataLoader(test_ds, batch_size=64, shuffle=False, num_workers=2)

    model = Net().to(device)
    opt = torch.optim.SGD(model.parameters(), lr=args.lr, momentum=args.momentum)
    crit = nn.CrossEntropyLoss()

    print(f"[Centralized] rounds={args.rounds} epochs_per_round={args.epochs} "
          f"batch={args.batch_size} lr={args.lr} subset={args.subset}")

    t0 = time.time()
    for rd in range(args.rounds):
        indices = random.sample(range(len(train_full)), args.subset)
        train_ds = Subset(train_full, indices)
        train_loader = DataLoader(train_ds, batch_size=args.batch_size, shuffle=True, num_workers=2, drop_last=True)

        print(f"[Round {rd+1}] using samples total {len(train_ds)}")

        # 每个 round 内训练 epochs 次
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
            print(f"Round {rd+1} Epoch {ep}: loss={loss_sum/len(train_loader):.4f}  acc={acc*100:.2f}%")

    print(f"[Centralized] total_time={time.time() - t0:.2f}s")

    torch.save(model.state_dict(), "mnist_centralized_rounds.pt")
    print("Saved: mnist_centralized_rounds.pt")

if __name__ == "__main__":
    main()
