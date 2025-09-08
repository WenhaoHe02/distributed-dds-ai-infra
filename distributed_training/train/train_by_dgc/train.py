import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, transforms
import torch.nn.functional as F
from torch.utils.data import DataLoader
import matplotlib.pyplot as plt
from dgc.compression import DGCCompressor
from dgc.memory import DGCSGDMemory


# 1. 数据准备
def load_mnist(batch_size=64):
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))  # MNIST的均值和标准差
    ])

    # 加载训练集和测试集
    train_set = datasets.MNIST(
        root='./data',
        train=True,
        download=True,
        transform=transform
    )
    test_set = datasets.MNIST(
        root='./data',
        train=False,
        download=True,
        transform=transform
    )

    # 创建数据加载器
    train_loader = DataLoader(
        train_set,
        batch_size=batch_size,
        shuffle=True,
        num_workers=4
    )
    test_loader = DataLoader(
        test_set,
        batch_size=batch_size,
        shuffle=False,
        num_workers=4
    )

    return train_loader, test_loader


# 2. 模型定义（和原代码相同）
class Net(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.pool = nn.MaxPool2d(2)
        self.fc1 = nn.Linear(64 * 12 * 12, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = F.relu(self.conv1(x))
        x = F.relu(self.conv2(x))
        x = self.pool(x)
        x = torch.flatten(x, 1)
        x = F.relu(self.fc1(x))
        return self.fc2(x)


# 3. 训练函数
def train_with_dgc(model, train_loader, optimizer, compressor, epoch, device):
    model.train()
    total_loss = 0

    # 第一次迭代时初始化
    if epoch == 1 and not hasattr(compressor.memory, 'momentum_buffers'):
        named_parameters = [(name, param) for name, param in model.named_parameters()]  # 新增这行
        compressor.initialize(named_parameters)  # 修改这行
        compressor.memory.initialize(named_parameters)  # 修改这行

    # 更新压缩率(热身训练)
    compressor.warmup_compress_ratio(epoch)

    for data, target in train_loader:
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.cross_entropy(output, target)
        loss.backward()

        # 对每个参数应用DGC压缩
        compressed_grads = {}
        for name, param in model.named_parameters():
            if param.grad is not None:
                # 压缩梯度
                compressed_grad, ctx = compressor.compress(param.grad, name)
                compressed_grads[name] = (compressed_grad, ctx)
        # 清空原始梯度
        optimizer.zero_grad()

        # 解压缩梯度并应用到参数
        for name, param in model.named_parameters():
            if name in compressed_grads:
                compressed_grad, ctx = compressed_grads[name]
                # 解压缩梯度
                decompressed_grad = compressor.decompress(compressed_grad, ctx)
                # 将解压缩后的梯度赋给参数
                param.grad = decompressed_grad

        optimizer.step()
        total_loss += loss.item()

    avg_loss = total_loss / len(train_loader)
    print(f'Train Epoch: {epoch} \tLoss: {avg_loss:.6f}')
    return avg_loss


# 4. 测试函数
def test(model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.cross_entropy(output, target, reduction='sum').item()
            pred = output.argmax(dim=1, keepdim=True)
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)
    accuracy = 100. * correct / len(test_loader.dataset)
    print(f'Test set: Average loss: {test_loss:.4f}, Accuracy: {correct}/{len(test_loader.dataset)} ({accuracy:.2f}%)')
    return accuracy

# 5. 保存模型参数
def save_checkpoint(epoch, model, optimizer, loss, path='checkpoint.pth'):
    torch.save({
        'epoch': epoch,
        'model_state_dict': model.state_dict(),
        'optimizer_state_dict': optimizer.state_dict(),
        'loss': loss,
    }, path)

    print("")


# 6. 主流程
def main():
    # 超参数
    batch_size = 64
    epochs = 10
    lr = 0.01
    momentum = 0.9
    best_accuracy = 0.0

    # 初始化参数
    compress_ratio = 0.01  # 压缩比例，例如1%
    nesterov = False  # 是否使用Nesterov动量

    # 创建DGC内存补偿器和压缩器
    memory = DGCSGDMemory(momentum=momentum, nesterov=nesterov)
    compressor = DGCCompressor(compress_ratio, memory=memory)

    # 设备配置
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # 加载数据
    train_loader, test_loader = load_mnist(batch_size)

    # 初始化模型和优化器
    model = Net().to(device)
    optimizer = optim.SGD(model.parameters(), lr=lr, momentum=momentum)

    named_parameters = [(name, param) for name, param in model.named_parameters()]
    # 初始化压缩器和内存
    compressor.initialize(named_parameters)
    memory.initialize(named_parameters)


    # 训练和测试循环
    train_losses = []
    test_accuracies = []
    for epoch in range(1, epochs + 1):
        optimizer.current_epoch = epoch  # 更新当前epoch
        loss = train_with_dgc(model, train_loader, optimizer, compressor, epoch, device)
        accuracy = test(model, device, test_loader)
        train_losses.append(loss)
        test_accuracies.append(accuracy)

        # 动态保存最佳模型
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            torch.save({
                'model_state_dict': model.state_dict(),
                'accuracy': accuracy,
                'epoch': epoch
            }, "best_model.pth")
            print(f"New best model saved at epoch {epoch} with accuracy {accuracy:.2f}%")

        # 在训练循环中调用：
        if epoch % 2 == 0:  # 每2个epoch保存一次
            save_checkpoint(epoch, model, optimizer, loss)

    # 可视化结果
    plt.figure(figsize=(12, 4))
    plt.subplot(1, 2, 1)
    plt.plot(train_losses, label='Training Loss')
    plt.xlabel('Epoch')
    plt.ylabel('Loss')
    plt.legend()

    plt.subplot(1, 2, 2)
    plt.plot(test_accuracies, label='Test Accuracy')
    plt.xlabel('Epoch')
    plt.ylabel('Accuracy (%)')
    plt.legend()
    plt.show()

    # 保存完整训练状态
    final_checkpoint = {
        'model': model.state_dict(),
        'optimizer': optimizer.state_dict(),
        'epoch': epochs,
        'loss_history': train_losses,
        'accuracy_history': test_accuracies
    }
    torch.save(final_checkpoint, 'mnist_final.pth')




if __name__ == '__main__':
    main()