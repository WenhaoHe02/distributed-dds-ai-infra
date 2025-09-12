# -*- coding: utf-8 -*-
# Int8 稠密压缩器：将梯度量化为int8进行传输
import torch
from memoryBaseline import Memory


class Int8CompressorBase:
    def __init__(self, memory=None, warmup_epochs=-1, warmup_coeff=None):
        self.world_size = 1  # DDP 下请在外部设置为实际 world_size
        self.op = "average"  # 简化：目标语义 = 平均

        self.memory = Memory() if memory is None else memory
        self.warmup_epochs = warmup_epochs
        self.warmup_enabled = warmup_epochs > 0

        if self.warmup_enabled:
            if warmup_coeff is None:
                self.warmup_coeff = 1.0  # 默认warmup期间不压缩
            else:
                if isinstance(warmup_coeff, (tuple, list)):
                    assert len(warmup_coeff) >= self.warmup_epochs
                    for wc in warmup_coeff:
                        assert 0 < wc <= 1
                else:
                    assert 0 < warmup_coeff <= 1
                self.warmup_coeff = warmup_coeff
        else:
            self.warmup_coeff = 1.0

        self.current_epoch = 0
        self.attributes = {}

    def set_world_size(self, world_size: int):
        self.world_size = int(world_size)

    def initialize(self, named_parameters):
        print("=> initializing int8 compressor")
        self.memory.initialize(named_parameters)

        for name, param in named_parameters:
            if torch.is_tensor(param):
                numel = param.numel()
                shape = list(param.size())
            else:
                assert isinstance(param, (list, tuple))
                numel, shape = param[0], param[1]

            self.attributes[name] = (numel, shape)
            print(f'   {name:<25}: int8 quantize {numel} elements of shape {shape}')

    def warmup_compress_ratio(self, epoch):
        """兼容原接口，但int8压缩不使用ratio概念"""
        self.current_epoch = epoch
        if self.warmup_enabled:
            if epoch < self.warmup_epochs:
                print(f'warmup epoch {epoch}: skip compression')
            else:
                print(f'epoch {epoch}: enable int8 compression')

    def _should_compress(self):
        """判断是否应该压缩"""
        if not self.warmup_enabled:
            return True
        return self.current_epoch >= self.warmup_epochs

    def _quantize_int8(self, tensor):
        """将tensor量化为int8"""
        # 计算缩放因子：使用绝对值最大值作为scale
        abs_max = tensor.abs().max()
        if abs_max == 0:
            # 全零张量情况
            scale = torch.tensor(1.0, dtype=tensor.dtype, device=tensor.device)
            quantized = torch.zeros_like(tensor, dtype=torch.int8)
        else:
            # 缩放到 [-127, 127] 范围
            scale = abs_max / 127.0
            quantized = torch.round(tensor / scale).clamp(-127, 127).to(torch.int8)

        return quantized, scale

    def _dequantize_int8(self, quantized, scale):
        """将int8反量化回float"""
        return quantized.float() * scale

    def compress(self, tensor, name):
        if not self._should_compress() or name not in self.attributes:
            # warmup期间或未知参数，不压缩
            ctx = (name, None, None, tensor.dtype, False, None)
            return tensor, ctx

        # 误差反馈补偿
        tensor_compensated = self.memory.compensate(tensor, name, accumulate=True)

        # Int8量化
        quantized, scale = self._quantize_int8(tensor_compensated)

        # 更新内存（误差积累）
        dequantized = self._dequantize_int8(quantized, scale)
        quantization_error = tensor_compensated - dequantized
        self.memory.update(name, quantization_error)

        # 返回压缩数据和上下文
        ctx = (name, tensor.numel(), list(tensor.shape), tensor.dtype, True, tensor.data)
        compressed_data = (quantized, scale)

        return compressed_data, ctx

    def decompress(self, tensor, ctx):
        name, numel, shape, orig_dtype, is_compressed, grad_buffer = ctx

        if not is_compressed:
            # 未压缩的情况
            return self.memory.compensate(tensor, name, accumulate=False)

        # 解压缩
        quantized, scale = tensor
        dequantized = self._dequantize_int8(quantized, scale)

        # 平均操作（DDP语义）
        if self.op == 'average' and self.world_size > 1:
            dequantized.mul_(1.0 / self.world_size)

        # 恢复原始形状
        if shape is not None:
            dequantized = dequantized.view(shape)

        return dequantized