# -*- coding: utf-8 -*-
# 将 DGC 压缩接入每 step 的 ZRDDS allgather（只计纯通信等待时间）
import logging, os, time
import numpy as np
import torch

logging.basicConfig(level=logging.INFO)

class DDPDGCStepper:
    """
    loss.backward()
    stepper.begin_step(step)
    stepper.finish_and_apply()
    optimizer.step(); optimizer.zero_grad()

    统计：
      - 纯网络等待时间（handle.wait 的阻塞时间，毫秒），不包含收集合并/解压
      - 窗口聚合打印（DGC_LOG_EVERY=N）；report_and_reset() 可在 epoch 末打印汇总
    """
    def __init__(self, model, compressor, zrdds_engine, group_id: str, rank: int, world: int,
                 dtype_val=np.float32, dtype_idx=np.int32):
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

        self._handles = []   # [(name, is_sparse, (h,) or (h0,h1))]
        self._ctx = []       # [(name, ctx, p)]

        self._txrx0 = (0, 0)  # bytes基线（含帧头）
        self._cur_step = -1
        self._last_dds_wait_ms = 0.0  # 本步纯 wait 总和（毫秒）

        # 窗口聚合
        self.log_every = int(os.environ.get("DGC_LOG_EVERY", "0"))
        self._win_ms = 0.0
        self._win_tx = 0
        self._win_rx = 0
        self._win_steps = 0
        self._ema_ms = None

    @staticmethod
    def _to_bytes(t: torch.Tensor, np_dtype):
        arr = t.detach().contiguous().to("cpu").numpy().astype(np_dtype, copy=False)
        return arr.tobytes(order="C")

    @staticmethod
    def _from_bytes(buf: bytes, np_dtype, torch_device):
        arr = np.frombuffer(buf, dtype=np_dtype)
        return torch.from_numpy(arr).to(torch_device)

    def _await_wait_only(self, h, timeout_ms):
        """仅等待（纯通信时间, 毫秒），不取数据。兼容 _AGHandle / 旧 tuple。"""
        if hasattr(h, "wait"):
            t0 = time.perf_counter()
            h.wait(timeout_ms)
            self._last_dds_wait_ms += (time.perf_counter() - t0) * 1000.0
            return None
        else:
            # 旧 tuple：无法拆分，只能把等待+收集合并一起算
            t0 = time.perf_counter()
            out = h[1](timeout_ms)
            self._last_dds_wait_ms += (time.perf_counter() - t0) * 1000.0
            return out

    def _await_and_collect(self, h, timeout_ms):
        """等待后返回数据（若是 _AGHandle，则先 wait 再 collect；否则直接返回 out）"""
        if hasattr(h, "collect"):
            self._await_wait_only(h, timeout_ms)
            return h.collect()
        else:
            return self._await_wait_only(h, timeout_ms)

    def begin_step(self, step_id: int):
        """请在 backward 后调用，保证 p.grad 已就绪"""
        self._cur_step = step_id
        self._last_dds_wait_ms = 0.0

        if hasattr(self.comm, "bytes_counters"):
            self._txrx0 = self.comm.bytes_counters()
        else:
            self._txrx0 = (0, 0)

        self._handles.clear()
        self._ctx.clear()

        # 压缩 + 发起异步 allgather（这里只是入队，不算“传输时间”）
        for name, p in self.named_params:
            if p.grad is None:
                continue
            compressed, ctx = self.comp.compress(p.grad.data, name)
            self._ctx.append((name, ctx, p))

            if isinstance(compressed, (tuple, list)):  # 稀疏 (values, indices)
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
            else:
                d_bytes = self._to_bytes(compressed.view(-1), self.dtype_val)
                h = self.comm.allgather_async(group_id=self.group, round_id=step_id,
                                              name=f"{name}.dense", part_id=0,
                                              rank=self.rank, world=self.world,
                                              payload=d_bytes)
                self._handles.append((name, False, (h,)))

    def finish_and_apply(self, timeout_s=10000.0):
        device = next(self.model.parameters()).device
        timeout_ms = None if timeout_s is None else int(timeout_s * 1000.0)

        # “通信阶段”的墙钟（供参考）
        t_wall0 = time.perf_counter()

        for (name, is_sparse, hs), (_, ctx, p) in zip(self._handles, self._ctx):
            if is_sparse:
                vals_lists = self._await_and_collect(hs[0], timeout_ms)
                idxs_lists = self._await_and_collect(hs[1], timeout_ms)

                # 解压/聚合（不计通信时间）
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
                dense_lists = self._await_and_collect(hs[0], timeout_ms)
                acc = None
                for vb in dense_lists:
                    v = self._from_bytes(vb, self.dtype_val, device).to(torch.float32)
                    acc = v if acc is None else acc.add_(v)
                acc.mul_(1.0 / self.world)
                p.grad.data.copy_(acc.view_as(p.grad.data))

        comm_wall_ms = (time.perf_counter() - t_wall0) * 1000.0  # 该阶段墙钟

        # 聚合统计
        dtx = drx = 0
        if hasattr(self.comm, "bytes_counters") and self._cur_step >= 0:
            tx1, rx1 = self.comm.bytes_counters()
            dtx = tx1 - self._txrx0[0]
            drx = rx1 - self._txrx0[1]

        self._win_ms   += self._last_dds_wait_ms
        self._win_tx   += int(dtx)
        self._win_rx   += int(drx)
        self._win_steps += 1

        if self.rank == 0 and self.log_every > 0 and self._win_steps >= self.log_every:
            avg_ms = self._win_ms / max(1, self._win_steps)
            mb_total = (self._win_tx + self._win_rx) / (1024.0 * 1024.0)
            mb_avg   = mb_total / self._win_steps
            self._ema_ms = avg_ms if self._ema_ms is None else (0.9 * self._ema_ms + 0.1 * avg_ms)
            step_lo = self._cur_step - self._win_steps + 1
            step_hi = self._cur_step
            logging.info(
                f"[DDS][{step_lo}-{step_hi}] wait_sum(avg)={avg_ms:.2f} ms  "
                f"phase_wall(last)={comm_wall_ms:.2f} ms  avg_bytes={mb_avg:.3f} MB  total_bytes={mb_total:.2f} MB"
            )
            self._win_ms = self._win_tx = self._win_rx = 0
            self._win_steps = 0

        return self._last_dds_wait_ms

    def report_and_reset(self, tag="epoch"):
        if self._win_steps == 0:
            return
        avg_ms = self._win_ms / max(1, self._win_steps)
        mb_total = (self._win_tx + self._win_rx) / (1024.0 * 1024.0)
        mb_avg   = mb_total / self._win_steps
        self._ema_ms = avg_ms if self._ema_ms is None else (0.9 * self._ema_ms + 0.1 * avg_ms)
        if self.rank == 0:
            logging.info(f"[DDS][{tag}] steps={self._win_steps} wait_sum(avg)={avg_ms:.2f} ms  "
                         f"ema={self._ema_ms:.2f} ms  avg_bytes={mb_avg:.3f} MB  total_bytes={mb_total:.2f} MB")
        self._win_ms = self._win_tx = self._win_rx = 0
        self._win_steps = 0
