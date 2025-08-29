import sys
import json
import numpy as np

def aggregate(updates):
    """
    updates: list of dicts, each like:
        {
          "client_id": int,
          "round_id": int,
          "num_samples": int,
          "weights": [float, float, ...]
        }
    return: dict for ModelBlob
    """

    # 确认所有 client 的 round_id 一致
    round_ids = [u["round_id"] for u in updates]
    if len(set(round_ids)) != 1:
        raise ValueError("All updates must have the same round_id")

    total_samples = sum(u["num_samples"] for u in updates)

    # 聚合（加权平均）
    agg_weights = None
    for u in updates:
        w = np.array(u["weights"], dtype=np.float64)
        weight_factor = u["num_samples"] / total_samples
        if agg_weights is None:
            agg_weights = w * weight_factor
        else:
            agg_weights += w * weight_factor

    return {
        "round_id": round_ids[0],
        "weights": agg_weights.tolist()
    }

if __name__ == "__main__":
    # 从 stdin 读取 JSON
    raw = sys.stdin.read()
    updates = json.loads(raw)

    result = aggregate(updates)

    # 输出聚合结果（JSON）
    print(json.dumps(result))
