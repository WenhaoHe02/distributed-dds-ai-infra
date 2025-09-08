import argparse
import torch
import torch.nn.functional as F
from torchvision import datasets, transforms
from torch.utils.data import DataLoader
from dist_train import Net  # 假设你的模型结构定义在 model.py 中
from torch.nn.utils import vector_to_parameters

# 全局缓存 loader
_cached_test_loader = None

def get_test_loader(data_dir, batch_size=128):
    global _cached_test_loader
    if _cached_test_loader is not None:
        return _cached_test_loader

    tfm = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    test_set = datasets.MNIST(data_dir, train=False, transform=tfm)
    loader = DataLoader(test_set, batch_size=batch_size, shuffle=False)
    _cached_test_loader = loader
    return loader

def evaluate_from_bytes(model_bytes: bytes, data_dir, batch_size=128, use_int8=False):
    device = torch.device("cpu")

    # 1. 数据加载（缓存）
    loader = get_test_loader(data_dir, batch_size)

    # 2. 模型加载
    model = Net().to(device)
    vec = torch.frombuffer(model_bytes, dtype=torch.float32)
    vector_to_parameters(vec, model.parameters())
    model.eval()

    # 3. 推理
    correct = 0
    total = 0
    with torch.no_grad():
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            if use_int8:
                x = (x * 255).to(torch.uint8)
            out = model(x.float())
            pred = out.argmax(dim=1)
            correct += (pred == y).sum().item()
            total += y.size(0)

    acc = correct / total
    print(f"[Evaluate] Accuracy: {acc:.4f} ({correct}/{total})")
    return acc

# 保留 CLI 调用
def evaluate(model_path, data_dir, batch_size=128, use_int8=False):
    with open(model_path, "rb") as f:
        model_bytes = f.read()
    return evaluate_from_bytes(model_bytes, data_dir, batch_size, use_int8)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True, help="模型参数文件（如某一轮的联邦聚合结果）")
    parser.add_argument("--data_dir", type=str, default="./data")
    parser.add_argument("--batch_size", type=int, default=128)
    parser.add_argument("--int8", action="store_true", help="是否模拟INT8输入（非真正量化）")
    args = parser.parse_args()

    evaluate(args.model, args.data_dir, args.batch_size, args.int8)
