import matplotlib.pyplot as plt
import numpy as np

# 数据定义
configs = ["fp32_dense", "int8_dense", "fp32_sparse", "int8_sparse"]
client_0_times = [547.30, 505.70, 450.20, 429.70]
client_1_times = [193.90, 92.00, 66.50, 65.10]
round_times    = [10484.80, 9982.20, 9432.70, 9589.00]
accuracies     = [0.9831, 0.9821, 0.9812, 0.9817]

x = np.arange(len(configs))
bar_width = 0.35

def annotate_bars(ax, bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width()/2, height),
                    xytext=(0, 4),
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=10, fontweight='bold')

def annotate_points(ax, x, y):
    for xi, yi in zip(x, y):
        ax.annotate(f'{yi:.4f}', xy=(xi, yi), xytext=(0, 6),
                    textcoords="offset points", ha='center',
                    fontsize=10, fontweight='bold', color='red')

# ===== 图 1: Client 通信时间 =====
fig1, ax1 = plt.subplots(figsize=(10, 6))
bar1 = ax1.bar(x - bar_width/2, client_0_times, width=bar_width, label="Client 0", color="skyblue")
bar2 = ax1.bar(x + bar_width/2, client_1_times, width=bar_width, label="Client 1", color="orange")
ax1.set_title("Client Communication Time per Compression Mode", fontsize=14)
ax1.set_ylabel("Time (ms)")
ax1.set_xticks(x)
ax1.set_xticklabels(configs)
ax1.legend()
ax1.grid(True, axis='y', linestyle='--', alpha=0.5)
annotate_bars(ax1, bar1)
annotate_bars(ax1, bar2)
plt.tight_layout()

# ===== 图 2: Round 总时间 =====
fig2, ax2 = plt.subplots(figsize=(10, 6))
bars = ax2.bar(x, round_times, color="blue", width=0.5)
ax2.set_title("Average Round Time per Compression Mode", fontsize=14)
ax2.set_ylabel("Time (ms)")
ax2.set_xticks(x)
ax2.set_xticklabels(configs)
ax2.grid(True, axis='y', linestyle='--', alpha=0.5)
annotate_bars(ax2, bars)
plt.tight_layout()

# ===== 图 3: 模型准确率 =====
fig3, ax3 = plt.subplots(figsize=(10, 6))
ax3.plot(x, accuracies, marker="o", color="red", linewidth=2)
ax3.set_title("Accuracy per Compression Mode", fontsize=14)
ax3.set_ylabel("Accuracy")
ax3.set_xticks(x)
ax3.set_xticklabels(configs)
ax3.set_ylim(0.980, 0.985)
ax3.grid(True, axis='y', linestyle='--', alpha=0.5)
annotate_points(ax3, x, accuracies)
plt.tight_layout()

plt.show()
