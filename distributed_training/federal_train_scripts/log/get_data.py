import re
from statistics import mean

logfile = "sq8.txt"

# 匹配 client 的通信时间
uplink_pattern = re.compile(r"transit≈(\d+) ms \(round=(\d+) client=(\d+)")
# 匹配每轮总时间
round_pattern = re.compile(r"round time: (\d+) ms")

client_times = {}  # {client_id: {round_id: [times]}}
round_times = {}   # {round_id: time}

with open(logfile, "r", encoding="utf-8") as f:
    for line in f:
        # 提取uplink
        m = uplink_pattern.search(line)
        if m:
            t, rnd, cid = int(m.group(1)), int(m.group(2)), int(m.group(3))
            client_times.setdefault(cid, {}).setdefault(rnd, []).append(t)
        # 提取round总时间
        m = round_pattern.search(line)
        if m:
            t = int(m.group(1))
            # 需要知道当前轮数，可以在前面有 "Round x" 记录
            # 这里简化为用最后一个 round= 的值
            # 更安全的方法是加一条 re 匹配 "Round (\d+)"
            round_times[rnd] = t

# 计算平均
print("=== 每个client的平均通信时间（10轮） ===")
for cid, rounds in client_times.items():
    all_times = [mean(times) for times in rounds.values()]  # 一个轮子可能多条取平均
    print(f"Client {cid}: {mean(all_times):.2f} ms")

print("\n=== 每轮总时间（10轮平均） ===")
print(f"{mean(round_times.values()):.2f} ms")
