import matplotlib.pyplot as plt
import numpy as np

# 固定顺序：Baseline 左，GDC 右
modes = ['Baseline', 'GDC']
x = np.arange(len(modes))
bar_width = 0.5

# 数据
avg_wait_ms  = [25.54, 20.83]
max_wait_ms  = [3025.49, 414.15]
avg_bytes_mb = [1.165, 0.010]
accuracy     = [98.07, 98.11]

def plot_single_bar(title, values, ylabel, unit, color, ylim=None, format_str="{:.2f}"):
    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.bar(x, values, width=bar_width, color=color)
    ax.set_title(title, fontsize=14)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels(modes, fontsize=11)
    ax.grid(True, axis='y', linestyle='--', alpha=0.5)
    if ylim:
        ax.set_ylim(*ylim)

    # 数值标注
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{format_str.format(height)} {unit}',
                    xy=(bar.get_x() + bar.get_width()/2, height),
                    xytext=(0, 5),
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.show()

# 图 1：avg wait
plot_single_bar(
    title="Average Communication Wait Time",
    values=avg_wait_ms,
    ylabel="Time (ms)",
    unit="ms",
    color="steelblue"
)

# 图 2：max wait
plot_single_bar(
    title="Max Communication Wait Time",
    values=max_wait_ms,
    ylabel="Max Time (ms)",
    unit="ms",
    color="darkblue",
    format_str="{:.0f}"
)

# 图 3：通信量
plot_single_bar(
    title="Average Communication Payload per Step",
    values=avg_bytes_mb,
    ylabel="Payload Size (MB)",
    unit="MB",
    color="mediumseagreen",
    format_str="{:.3f}"
)

# 图 4：验证精度
plot_single_bar(
    title="Validation Accuracy",
    values=accuracy,
    ylabel="Accuracy (%)",
    unit="%",
    color="indianred",
    ylim=(97.5, 98.3),
    format_str="{:.2f}"
)
