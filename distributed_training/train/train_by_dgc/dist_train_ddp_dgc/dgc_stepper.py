# -*- coding: utf-8 -*-
# 将 DGC 压缩接入每 step 的 ZRDDS allgather
import numpy as np
import torch

class DDPDGCStepper:
    """
    用法：
      stepper = DDPDGCStepper(model, compressor, zrdds, group_id, rank, world)
      for step in ...:
          stepper.begin_step(step)
          loss.backward()
          stepper.finish_and_apply()
          optimizer.step(); optimizer.zero_grad()
    """
    def __init__(self, model, compressor, zrdds_engine, group_id:str, rank:int, world:int,
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

        self._handles = []
        self._ctx = []

    @staticmethod
    def _to_bytes(t: torch.Tensor, np_dtype):
        arr = t.detach().contiguous().to("cpu").numpy().astype(np_dtype, copy=False)
        return arr.tobytes(order="C")

    @staticmethod
    def _from_bytes(buf: bytes, np_dtype, torch_device):
        arr = np.frombuffer(buf, dtype=np_dtype)
        return torch.from_numpy(arr).to(torch_device)

    def begin_step(self, step_id:int):
        self._handles.clear(); self._ctx.clear()
        for name, p in self.named_params:
            if p.grad is None:
                continue
            compressed, ctx = self.comp.compress(p.grad.data, name)
            self._ctx.append((name, ctx, p))
            if isinstance(compressed, (tuple, list)):
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

    def finish_and_apply(self, timeout_s=60.0):
        device = next(self.model.parameters()).device
        for (name, is_sparse, hs), (_, ctx, p) in zip(self._handles, self._ctx):
            if is_sparse:
                vals_lists = hs[0].wait(timeout_s)  # 按 rank 排序的 bytes 列表
                idxs_lists = hs[1].wait(timeout_s)
                numel = ctx[1]; shape = ctx[2]
                dense = torch.zeros(numel, dtype=torch.float32, device=device)
                # vals_lists 和 idxs_lists 的长度 = world，每个元素是对应 rank 的 payload（已拼好 seq）
                for vb, ib in zip(vals_lists, idxs_lists):
                    v = self._from_bytes(vb, self.dtype_val, device).view(-1).to(torch.float32)
                    i = self._from_bytes(ib, self.dtype_idx, device).view(-1).to(torch.int64)
                    dense.index_put_([i], v, accumulate=True)
                dense.mul_(1.0 / self.world)  # AVG
                p.grad.data = dense.view(shape)
            else:
                dense_lists = hs[0].wait(timeout_s)
                acc = None
                for vb in dense_lists:
                    v = self._from_bytes(vb, self.dtype_val, device).to(torch.float32)
                    acc = v if acc is None else acc.add_(v)
                acc.mul_(1.0 / self.world)
                p.grad.data.copy_(acc.view_as(p.grad.data))
