# -*- coding: utf-8 -*-
# 全局精度评估（Top-1）：每个 rank 统计本地 correct/total，
# 用 ZrddsAllgather 汇总 -> 计算全局准确率
import struct
import torch

# 2 个 int64: (correct, total)
_FMT = "<qq"
_SIZE = struct.calcsize(_FMT)

def _pack_counts(correct: int, total: int) -> bytes:
    return struct.pack(_FMT, int(correct), int(total))

def _unpack_counts(b: bytes):
    assert len(b) == _SIZE, f"bad metric frame size: {len(b)}"
    return struct.unpack(_FMT, b)

def ddp_reduce_accuracy_counts(zrdds, group_id: str, round_id: int,
                               name: str, rank: int, world: int,
                               correct: int, total: int, timeout_s: float = 30.0):
    """
    用 ZrddsAllgather 汇总 (correct,total)。返回 (global_correct, global_total, acc)
    注意：round_id/name 要和训练用的 key 空间“错开”，避免冲突。
    """
    payload = _pack_counts(correct, total)
    # part_id=0 固定；name 用专属前缀，避免和梯度的 name 撞车
    h = zrdds.allgather_async(group_id=group_id, round_id=round_id,
                              name=f"metric.{name}", part_id=0,
                              rank=rank, world=world, payload=payload)
    frames = h[1](timeout_s)  # list[bytes] from all ranks（按 rank 排序）
    g_correct = 0
    g_total = 0
    for fb in frames:
        c, t = _unpack_counts(fb)
        g_correct += c
        g_total += t
    acc = (g_correct / g_total) if g_total > 0 else 0.0
    return g_correct, g_total, acc

@torch.no_grad()
def ddp_evaluate_top1(model: torch.nn.Module, dataloader, device,
                      zrdds, group_id: str, epoch_or_step: int,
                      name: str, rank: int, world: int, timeout_s: float = 60.0):
    """
    在 dataloader 上评估 Top-1，并做全局汇总。
    epoch_or_step 会作为 round_id（建议和训练 step 空间分离，见下方用法）。
    name 例如: "val.top1"
    """
    was_training = model.training
    model.eval()

    total = 0
    correct = 0
    for xb, yb in dataloader:
        xb = xb.to(device, non_blocking=True)
        yb = yb.to(device, non_blocking=True)
        logits = model(xb)
        pred = logits.argmax(dim=1)
        correct += (pred == yb).sum().item()
        total   += yb.numel()

    # 全局汇总
    g_correct, g_total, acc = ddp_reduce_accuracy_counts(
        zrdds, group_id, epoch_or_step, name, rank, world, correct, total, timeout_s
    )
    if was_training:
        model.train()
    return g_correct, g_total, acc
