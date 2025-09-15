# -*- coding: utf-8 -*-
# 将 DGC 压缩接入每 step 的 ZRDDS allgather
import logging

import numpy as np
import torch
import time
logging.basicConfig(level=logging.INFO)
class DDPDGCStepper:
    """
    用法（建议在 backward 之后再 begin_step）：
      stepper = DDPDGCStepper(model, compressor, zrdds, group_id, rank, world)
      for step in ...:
          loss.backward()
          stepper.begin_step(step)
          stepper.finish_and_apply()
          optimizer.step(); optimizer.zero_grad()

    特性：
      - 仅统计 DDS 等待时间（allgather 句柄 wait 的阻塞时间）
      - 按窗口聚合打印（环境变量 DGC_LOG_EVERY 控制每 N 步打印一次；0 表示不按步打印）
      - 可在 epoch 末调用 report_and_reset(tag=...) 打一条汇总
    """
    def __init__(self, model, compressor, zrdds_engine, group_id: str, rank: int, world: int,
                 dtype_val=np.float32, dtype_idx=np.int32):
        import os
        self.model = model
        self.comp = compressor
        self.comm = zrdds_engine
        self.group = group_id
        self.rank = int(rank)
        self.world = int(world)
        self.dtype_val = dtype_val
        self.dtype_idx = dtype_idx

        self.named_params = [(n, p) for n, p in model.named_parameters() if p.requires_grad]
        self.comp.set_world_size(self.world)
        self.comp.initialize(self.named_params)
        if hasattr(self.comp.memory, "initialize"):
            self.comp.memory.initialize(self.named_params)

        # 句柄/上下文
        self._handles = []  # [(name, is_sparse, (h0, h1 or h))]
        self._ctx = []      # [(name, ctx, p)]

        # 通信字节统计基线（来自 zrdds_engine.bytes_counters()）
        self._txrx0 = (0, 0)

        # 当前步 & 本步 DDS 等待时间
        self._cur_step = -1
        self._last_dds_wait_ms = 0.0

        # --- 聚合打印窗口 ---
        # 环境变量 DGC_LOG_EVERY 控制每多少步打印一次窗口统计；=0 表示不按步打印，仅手动 report_and_reset
        self.log_every = int(os.environ.get("DGC_LOG_EVERY", "0"))
        self._win_ms = 0.0
        self._win_tx = 0
        self._win_rx = 0
        self._win_steps = 0
        self._ema_ms = None  # 平滑显示

    @staticmethod
    def _to_bytes(t: torch.Tensor, np_dtype):
        arr = t.detach().contiguous().to("cpu").numpy().astype(np_dtype, copy=False)
        return arr.tobytes(order="C")

    @staticmethod
    def _from_bytes(buf: bytes, np_dtype, torch_device):
        arr = np.frombuffer(buf, dtype=np_dtype)
        return torch.from_numpy(arr).to(torch_device)

    def _await_handle(self, h, timeout_s):
        """只在等待阶段计时（即 DDS 通信的阻塞时间）"""
        t0 = time.perf_counter()
        if hasattr(h, "wait"):
            out = h.wait(timeout_s)
        else:
            # 兼容 (key, await_fn) 形式
            out = h[1](timeout_s)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        self._last_dds_wait_ms += dt_ms
        return out

    def begin_step(self, step_id: int):
        """注意：应在 loss.backward() 之后调用，保证 p.grad 已就绪"""
        self._cur_step = step_id
        self._last_dds_wait_ms = 0.0

        # 开步基线字节
        if hasattr(self.comm, "bytes_counters"):
            self._txrx0 = self.comm.bytes_counters()
        else:
            self._txrx0 = (0, 0)

        # 清空句柄/上下文
        self._handles.clear()
        self._ctx.clear()

        # 压缩 + 发起异步 allgather
        for name, p in self.named_params:
            if p.grad is None:
                continue
            compressed, ctx = self.comp.compress(p.grad.data, name)
            self._ctx.append((name, ctx, p))

            if isinstance(compressed, (tuple, list)):  # 稀疏(values, indices)
                values, indices = compressed
                v_bytes = self._to_bytes(values.view(-1), self.dtype_val)
                i_bytes = self._to_bytes(indices.view(-1), self.dtype_idx)

                h0 = self.comm.allgather_async(group_id=self.group, round_id=step_id,
                                               name=f"{name}.t0", part_id=0,
                                               rank=self.rank, world=self.world,
                                               payload=v_bytes)
                h1 = self.comm.allgather_async(group_id=self.group, round_id=step_id,
                                               name=f"{name}.t1", part_id=1,
                                               rank=self.rank, world=self.world,
                                               payload=i_bytes)
                self._handles.append((name, True, (h0, h1)))
            else:  # 稠密
                d_bytes = self._to_bytes(compressed.view(-1), self.dtype_val)
                h = self.comm.allgather_async(group_id=self.group, round_id=step_id,
                                              name=f"{name}.dense", part_id=0,
                                              rank=self.rank, world=self.world,
                                              payload=d_bytes)
                self._handles.append((name, False, (h,)))

    def finish_and_apply(self, timeout_s=10000.0):
        device = next(self.model.parameters()).device

        # 等待通信 + 解压聚合
        for (name, is_sparse, hs), (_, ctx, p) in zip(self._handles, self._ctx):
            if is_sparse:
                # 仅等待阶段计时
                vals_lists = self._await_handle(hs[0], timeout_s)
                idxs_lists = self._await_handle(hs[1], timeout_s)

                # 解压：一次性拼接再 index_put，避免多次 index_put
                numel = ctx[1]; shape = ctx[2]
                dense = torch.zeros(numel, dtype=torch.float32, device=device)
                v_cat = torch.cat(
                    [self._from_bytes(vb, self.dtype_val, device).view(-1).to(torch.float32)
                     for vb in vals_lists],
                    dim=0
                )
                i_cat = torch.cat(
                    [self._from_bytes(ib, self.dtype_idx, device).view(-1).to(torch.int64)
                     for ib in idxs_lists],
                    dim=0
                )
                dense.index_put_([i_cat], v_cat, accumulate=True)
                dense.mul_(1.0 / self.world)
                p.grad.data = dense.view(shape)
            else:
                # 稠密直接求和平均
                dense_lists = self._await_handle(hs[0], timeout_s)
                acc = None
                for vb in dense_lists:
                    v = self._from_bytes(vb, self.dtype_val, device).to(torch.float32)
                    acc = v if acc is None else acc.add_(v)
                acc.mul_(1.0 / self.world)
                p.grad.data.copy_(acc.view_as(p.grad.data))

        # 聚合统计（窗口内累计）
        dtx = drx = 0
        if hasattr(self.comm, "bytes_counters") and self._cur_step >= 0:
            tx1, rx1 = self.comm.bytes_counters()
            dtx = tx1 - self._txrx0[0]
            drx = rx1 - self._txrx0[1]

        self._win_ms   += self._last_dds_wait_ms
        self._win_tx   += int(dtx)
        self._win_rx   += int(drx)
        self._win_steps += 1

        # 按窗口打印（仅 rank0）
        if self.rank == 0 and self.log_every > 0 and self._win_steps >= self.log_every:
            avg_ms = self._win_ms / max(1, self._win_steps)
            mb_total = (self._win_tx + self._win_rx) / (1024.0 * 1024.0)
            mb_avg   = mb_total / self._win_steps
            self._ema_ms = avg_ms if self._ema_ms is None else (0.9 * self._ema_ms + 0.1 * avg_ms)
            step_lo = self._cur_step - self._win_steps + 1
            step_hi = self._cur_step
            logging.info(f"[DDS][{step_lo}-{step_hi}] avg_wait={avg_ms:.2f} ms  ema={self._ema_ms:.2f} ms  "
                         f"avg_bytes={mb_avg:.3f} MB  total_bytes={mb_total:.2f} MB")
            # 重置窗口
            self._win_ms = self._win_tx = self._win_rx = 0
            self._win_steps = 0

    def report_and_reset(self, tag="epoch"):
        """在 epoch 末调用：打印窗口内的汇总并清零窗口计数"""
        if self._win_steps == 0:
            return
        avg_ms = self._win_ms / max(1, self._win_steps)
        mb_total = (self._win_tx + self._win_rx) / (1024.0 * 1024.0)
        mb_avg   = mb_total / self._win_steps
        self._ema_ms = avg_ms if self._ema_ms is None else (0.9 * self._ema_ms + 0.1 * avg_ms)
        if self.rank == 0:
            logging.info(f"[DDS][{tag}] steps={self._win_steps} avg_wait={avg_ms:.2f} ms  "
                         f"ema={self._ema_ms:.2f} ms  avg_bytes={mb_avg:.3f} MB  total_bytes={mb_total:.2f} MB")
        self._win_ms = self._win_tx = self._win_rx = 0
        self._win_steps = 0
