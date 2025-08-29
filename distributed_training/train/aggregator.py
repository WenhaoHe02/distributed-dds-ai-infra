import sys
import json
import base64
import numpy as np

def aggregate(updates):
    round_ids = [u["round_id"] for u in updates]
    if len(set(round_ids)) != 1:
        raise ValueError("All updates must have the same round_id")

    total_samples = sum(u["num_samples"] for u in updates)

    agg_weights = None
    for u in updates:
        # 解码 Base64 -> bytes -> float32
        weights_bytes = base64.b64decode(u["weights_b64"])
        w = np.frombuffer(weights_bytes, dtype=np.float32)

        weight_factor = u["num_samples"] / total_samples
        if agg_weights is None:
            agg_weights = w * weight_factor
        else:
            agg_weights += w * weight_factor

    # 返回时再编码为 Base64，Java 再 decode
    return {
        "round_id": round_ids[0],
        "weights_b64": base64.b64encode(agg_weights.astype(np.float32).tobytes()).decode("utf-8")
    }

if __name__ == "__main__":
    raw = sys.stdin.read()
    updates = json.loads(raw)
    result = aggregate(updates)
    print(json.dumps(result))
