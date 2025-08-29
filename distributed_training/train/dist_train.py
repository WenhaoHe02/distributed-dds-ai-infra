import argparse, random, json
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client_id", type=int, required=True)
    ap.add_argument("--seed",      type=int, required=True)
    ap.add_argument("--subset",    type=int, required=True)
    ap.add_argument("--epochs",    type=int, required=True)
    ap.add_argument("--lr",        type=float, required=True)  # Java 侧传 double，这里接 float 没问题
    ap.add_argument("--batch_size",type=int, default=128)
    ap.add_argument("--data_dir",  type=str, default="./data")
    ap.add_argument("--out",       type=str, required=True)
    args = ap.parse_args()

    tfm = transforms.Compose([transforms.ToTensor(),
                              transforms.Normalize((0.1307,), (0.3081,))])
    train_full = datasets.MNIST(args.data_dir, train=True, download=True, transform=tfm)

    random.seed(args.seed + args.client_id)
    idx = list(range(len(train_full))); random.shuffle(idx)
    idx = idx[:args.subset]
    ds = Subset(train_full, idx)

    loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True, num_workers=4, drop_last=True)
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

    print(json.dumps({"num_samples": len(ds), "bytes": len(raw)}))

if __name__ == "__main__":
    main()
