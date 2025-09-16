import logging
import os
import time

import numpy as np
import torch
import struct

class DDPDGCStepperBase:
    """
    DDP + DGC 修复版本：支持量化梯度传输并保留 scale
    """
    def __init__(self, model, zrdds_engine, group_id: str, rank: int, world: int,
                 dtype_val=np.int8, dtype_scale=np.float32):
        self.model = model
        #self.comp = compressor
        self.comm = zrdds_engine
        self.group = group_id
        self.rank = int(rank)
        self.world = int(world)
        self.dtype_val = dtype_val
        self.dtype_scale = dtype_scale

        self.named_params = [(n, p) for n, p in model.named_parameters()]
        #self.comp.set_world_size(self.world)
        #self.comp.initialize(self.named_params)
        self._last_dds_wait_ms = 0.0  # 本步纯 wait 总和（毫秒）

        self._handles = []
        self._ctx_map = {}

        self._txrx0 = (0, 0)  # bytes基线（含帧头）
        self._cur_step = -1
        self.log_every = int(os.environ.get("DGC_LOG_EVERY", "100"))
        self._win_ms = 0.0
        self._win_tx = 0
        self._win_rx = 0
        self._win_pl_tx = 0
        self._win_pl_rx = 0
        self._win_steps = 0
        self._ema_ms = None

    @staticmethod
    def _pack_data_with_scale(compressed_tensor: torch.Tensor, scale: float, dtype_val, dtype_scale):
        """将量化数据和scale打包成bytes"""
        val_bytes = compressed_tensor.detach().cpu().numpy().astype(dtype_val).tobytes(order="C")
        scale_bytes = struct.pack('<f', float(scale))
        return scale_bytes + val_bytes

    @staticmethod
    def _unpack_data_with_scale(buf: bytes, expected_size: int, dtype_val, torch_device):
        """从bytes解包出量化数据和scale"""
        scale = struct.unpack('<f', buf[:4])[0]
        val_arr = np.frombuffer(buf[4:], dtype=dtype_val)
        if len(val_arr) != expected_size:
            raise ValueError(f"Expected {expected_size} elements, got {len(val_arr)}")
        val_tensor = torch.from_numpy(val_arr).to(torch_device)
        return val_tensor, scale

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
        """压缩并发送每个梯度。必须在 loss.backward() 后调用"""
        self._cur_step = step_id
        self._handles.clear()
        self._ctx_map.clear()
        if hasattr(self.comm, "bytes_counters"):
            self._txrx0 = self.comm.bytes_counters()
        else:
            self._txrx0 = (0, 0)
        if hasattr(self.comm, "payload_counters"):
            self._pl0 = self.comm.payload_counters()
        else:
            self._pl0 = (0, 0)
        self._last_dds_wait_ms = 0.0

        for name, p in self.named_params:
            if p.grad is None:
                logging.info(f"[begin_step] skip {name}: grad is None")
                continue

            grad = p.grad.detach().clone()
            numel = grad.numel()
            shape = list(grad.shape)

            # int8 量化示例
            max_abs = float(grad.abs().max().item()) if numel > 0 else 0.0
            if max_abs == 0.0:
                scale = 1.0
                val_int8 = torch.zeros(numel, dtype=torch.int8)
            else:
                scale = max_abs / 127.0
                val_int8 = (grad.flatten() / scale).round().clamp(-128, 127).to(torch.int8)

            self._ctx_map[name] = (numel, shape, grad.dtype)

            payload = self._pack_data_with_scale(val_int8, scale, self.dtype_val, self.dtype_scale)

            #start_time = time.perf_counter()
            # 异步发送
            h = self.comm.allgather_async(
                group_id=self.group,
                round_id=step_id,
                name=f"{name}.dense",
                part_id=0,
                rank=self.rank,
                world=self.world,
                payload=payload,
                max_chunk=1 << 20
            )
            self._handles.append((name, h))
            #end_time = time.perf_counter()
            #total_t+=end_time-start_time

            logging.info(f"[Dense] Sent {name}: {payload.__sizeof__()} bytes, numel={numel}, scale={scale:.6f}")

        #logging.info(f"[Timing] begin_step step_id={step_id} transmission took {total_t:.6f} seconds")
        #return total_t

    def finish_and_apply(self, timeout_s=10000.0):
        """等待接收所有rank的梯度 -> 反量化 -> 累加平均 -> 应用回 p.grad"""
        device = next(self.model.parameters()).device
        timeout_ms = int(timeout_s * 1000)
        #total_t=0.0

        t_wall0 = time.perf_counter()

        for name, h in self._handles:
            if name not in self._ctx_map:
                raise RuntimeError(f"[finish] missing ctx for {name}")

            numel, shape, orig_dtype = self._ctx_map[name]

            # start_time = time.perf_counter()
            # # allgather_async 返回 (key, await_fn)
            # if isinstance(h, (tuple, list)) and len(h) == 2 and callable(h[1]):
            #     _, await_fn = h
            #     dense_lists = await_fn(timeout_ms)
            # elif callable(h):
            #     dense_lists = h(timeout_ms)
            # else:
            #     dense_lists = h
            # end_time = time.perf_counter()
            # total_t+=end_time-start_time

            dense_lists = self._await_and_collect(h, timeout_ms)

            accumulated = None
            per_rank_lengths = []

            for rank_idx, packed_buf in enumerate(dense_lists):
                val_tensor, scale = self._unpack_data_with_scale(packed_buf, numel, self.dtype_val, device)
                per_rank_lengths.append(val_tensor.numel())

                if val_tensor.numel() != numel:
                    if val_tensor.numel() < numel:
                        pad = torch.zeros(numel - val_tensor.numel(), device=device, dtype=val_tensor.dtype)
                        val_tensor = torch.cat([val_tensor, pad], dim=0)
                    else:
                        val_tensor = val_tensor[:numel]

                decompressed = val_tensor.to(torch.float32) * float(scale)
                accumulated = decompressed if accumulated is None else accumulated.add_(decompressed)

            if accumulated is None:
                raise RuntimeError(f"[finish] no data received for {name}")

            accumulated.mul_(1.0 / self.world)

            try:
                accumulated = accumulated.view(shape).to(orig_dtype)
                # 应用回梯度
                p = dict(self.named_params)[name]
                p.grad.data.copy_(accumulated)
                #logging.info(f"[Dense] Applied averaged gradient to {name}: shape={shape}, per_rank_lengths={per_rank_lengths}")
            except Exception as e:
                raise RuntimeError(
                    f"[finish] cannot view accumulated ({accumulated.numel()}) as {shape} for param {name}, per_rank_lengths={per_rank_lengths}"
                ) from e

        comm_wall_ms = (time.perf_counter() - t_wall0) * 1000.0  # 该阶段墙钟

        dtx = drx = 0
        if hasattr(self.comm, "bytes_counters") and self._cur_step >= 0:
            tx1, rx1 = self.comm.bytes_counters()
            dtx = tx1 - self._txrx0[0]
            drx = rx1 - self._txrx0[1]

        if hasattr(self.comm, "payload_counters") and self._cur_step >= 0:
            ptx1, prx1 = self.comm.payload_counters()
            self._win_pl_tx += int(ptx1 - self._pl0[0])
            self._win_pl_rx += int(prx1 - self._pl0[1])

        self._win_ms += self._last_dds_wait_ms
        self._win_tx += int(dtx)
        self._win_rx += int(drx)
        self._win_steps += 1

        if self.rank == 0 and self.log_every > 0 and self._win_steps >= self.log_every:
            avg_ms = self._win_ms / max(1, self._win_steps)
            mb_total = (self._win_tx + self._win_rx) / (1024.0 * 1024.0)
            mb_avg = mb_total / self._win_steps

            pl_mb_total = (self._win_pl_tx + self._win_pl_rx) / (1024.0 * 1024.0)
            pl_mb_avg = pl_mb_total / self._win_steps

            self._ema_ms = avg_ms if self._ema_ms is None else (0.9 * self._ema_ms + 0.1 * avg_ms)
            step_lo = self._cur_step - self._win_steps + 1
            step_hi = self._cur_step
            logging.info(
                f"[DDS][{step_lo}-{step_hi}] wait_sum(avg)={avg_ms:.2f} ms  "
                f"phase_wall(last)={comm_wall_ms:.2f} ms  "
                f"avg_bytes={mb_avg:.3f} MB  total_bytes={mb_total:.2f} MB  "
                f"avg_payload={pl_mb_avg:.3f} MB  total_payload={pl_mb_total:.2f} MB"
            )
            self._win_ms = self._win_tx = self._win_rx = 0
            self._win_pl_tx = self._win_pl_rx = 0
            self._win_steps = 0

        #logging.info(f"[Timing]  finish_and_apply transmission took {total_t:.6f} seconds")
        return self._last_dds_wait_ms
