# -*- coding: utf-8 -*-
import torch


class Memory:
    @staticmethod
    def initialize(*args, **kwargs): pass

    @staticmethod
    def compensate(tensor, *args, **kwargs): return tensor

    @staticmethod
    def update(*args, **kwargs): pass

    @staticmethod
    def state_dict(): return None

    @staticmethod
    def load_state_dict(state_dict): pass


class Int8SGDMemory(Memory):
    """ Int8 压缩的动量修正 + 误差反馈缓存 """

    def __init__(self, momentum=0.9, nesterov=False, gradient_clipping=None):
        self.gradient_clipping = gradient_clipping
        self.momentum = momentum
        self.nesterov = nesterov
        self.momentums = {}
        self.velocities = {}  # 用于误差反馈
        self.error_feedback = {}  # 量化误差累积

    def initialize(self, named_parameters):
        for name, p in named_parameters:
            if torch.is_tensor(p):
                param_data = p.data
            else:
                # 处理 (numel, shape) 的情况
                numel, shape = p
                param_data = torch.zeros(shape)

            self.momentums[name] = torch.zeros_like(param_data)
            self.velocities[name] = torch.zeros_like(param_data)
            self.error_feedback[name] = torch.zeros_like(param_data)

    def compensate(self, grad, name, accumulate=True):
        if self.gradient_clipping is not None:
            grad = self.gradient_clipping(grad)

        # 添加误差反馈
        if name in self.error_feedback:
            grad = grad + self.error_feedback[name]

        mmt = self.momentums[name]
        if accumulate:
            vec = self.velocities[name]
            if self.nesterov:
                mmt.add_(grad).mul_(self.momentum)
                vec.add_(mmt).add_(grad)
            else:
                mmt.mul_(self.momentum).add_(grad)
                vec.add_(mmt)
            return vec
        else:
            if self.nesterov:
                mmt.add_(grad).mul_(self.momentum)
                return mmt.add(grad)
            else:
                mmt.mul_(self.momentum).add_(grad)
                return mmt.clone()

    def update(self, name, quantization_error):
        """更新量化误差反馈"""
        if name in self.error_feedback:
            # 累积量化误差用于下次补偿
            self.error_feedback[name].copy_(quantization_error)

            # 对于稠密压缩，我们不需要清零动量和速度
            # （与稀疏DGC不同，int8是稠密压缩）

    def state_dict(self):
        return dict(
            momentums=self.momentums,
            velocities=self.velocities,
            error_feedback=self.error_feedback
        )

    def load_state_dict(self, state_dict):
        momentums = state_dict.get('momentums', {})
        velocities = state_dict.get('velocities', {})
        error_feedback = state_dict.get('error_feedback', {})

        for name in self.momentums.keys():
            if name in momentums:
                self.momentums[name] = momentums[name]
            if name in velocities:
                self.velocities[name] = velocities[name]
            if name in error_feedback:
                self.error_feedback[name] = error_feedback[name]