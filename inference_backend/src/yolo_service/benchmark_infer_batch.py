# yolo_bench_batch.py
import argparse, os, time, csv, math
from pathlib import Path
from ultralytics import YOLO
import torch
import matplotlib.pyplot as plt

# --------- 工具函数 ---------
def list_images(path):
    p = Path(path)
    exts = {".jpg",".jpeg",".png",".bmp",".webp"}
    if p.is_dir():
        files = [str(x) for x in sorted(p.iterdir()) if x.suffix.lower() in exts]
    elif p.is_file():
        files = [str(p)]
    else:
        files = []
    if not files:
        raise FileNotFoundError(f"No images under: {path}")
    return files

def chunks(lst, n):
    """yield successive n-sized chunks"""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def sync(device):
    if device == "cuda" and torch.cuda.is_available():
        torch.cuda.synchronize()

# --------- 基准逻辑 ---------
def run_bench(
    model_path: str,
    source_path: str,
    out_dir: str,
    batches,
    iters: int,
    warmup: int,
    imgsz: int,
    device: str,
    half: bool,
    conf: float,
    iou: float,
    max_det: int,
    verbose: bool
):
    device = ("cuda" if (device == "cuda" and torch.cuda.is_available()) else "cpu")
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)

    # 载入 YOLO
    model = YOLO(model_path)
    # 加速小技巧：融合 Conv+BN
    try:
        model.fuse()
    except Exception:
        pass

    # half 仅在 cuda 时有效
    use_half = bool(half and device == "cuda")

    # 准备输入列表（固定同一批数据，用于不同 batch 反复对比）
    files = list_images(source_path)

    # 为保证每个 batch size 都能整除批次数，复制数据到至少 batch_size * iters 张
    def repeat_to_len(lst, target_len):
        if len(lst) >= target_len: return lst[:target_len]
        k = math.ceil(target_len / len(lst))
        return (lst * k)[:target_len]

    results = []

    for bs in sorted(set(batches)):
        total_batches = iters
        total_imgs_needed = bs * total_batches
        files_bs = repeat_to_len(files, total_imgs_needed)

        # 预热：避免首次内核编译/缓存等干扰
        for _ in range(warmup):
            for batch_paths in chunks(files_bs[:bs], bs):
                _ = model.predict(
                    source=batch_paths,
                    imgsz=imgsz,
                    conf=conf,
                    iou=iou,
                    max_det=max_det,
                    device=(0 if device=="cuda" else "cpu"),
                    half=use_half,
                    stream=False,      # 一次性返回结果列表，内部按 batch 处理
                    save=False,
                    verbose=False
                )
                sync(device)

        # 正式计时
        t0 = time.perf_counter()
        img_cnt = 0
        for _ in range(total_batches):
            for batch_paths in chunks(files_bs[img_cnt:img_cnt+bs], bs):
                _ = model.predict(
                    source=batch_paths,
                    imgsz=imgsz,
                    conf=conf,
                    iou=iou,
                    max_det=max_det,
                    device=(0 if device=="cuda" else "cpu"),
                    half=use_half,
                    stream=False,
                    save=False,
                    verbose=False
                )
                img_cnt += len(batch_paths)
        sync(device)
        t1 = time.perf_counter()

#         assert img_cnt == total_imgs_needed
        total_s = t1 - t0
        avg_latency_batch_ms = (total_s / total_batches) * 1000.0
        avg_latency_img_ms   = (total_s / img_cnt)        * 1000.0
        throughput_ips       = img_cnt / total_s

        row = {
            "batch_size": bs,
            "avg_latency_batch_ms": round(avg_latency_batch_ms, 4),
            "avg_latency_img_ms": round(avg_latency_img_ms, 4),
            "throughput_img_per_s": round(throughput_ips, 2),
            "device": device,
            "half": use_half,
            "imgsz": imgsz,
            "conf": conf,
            "iou": iou,
            "max_det": max_det,
            "iters": iters,
            "warmup": warmup
        }
        results.append(row)
        print(f"[{device}/{'fp16' if use_half else 'fp32'}] "
              f"bs={bs:4d}  batch_lat={row['avg_latency_batch_ms']:8.3f} ms  "
              f"img_lat={row['avg_latency_img_ms']:7.3f} ms  thr={row['throughput_img_per_s']:9.1f} img/s")

    # 写 CSV
    csv_path = out / "yolo_batch_bench.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader(); writer.writerows(results)
    print(f"Saved CSV: {csv_path}")

    # 绘图（两张独立图）
    bs_list = [r["batch_size"] for r in results]
    lat_img = [r["avg_latency_img_ms"] for r in results]
    thrput  = [r["throughput_img_per_s"] for r in results]

    plt.figure()
    plt.title(f"YOLO per-image latency vs batch (device={device}, half={use_half})")
    plt.xlabel("Batch size"); plt.ylabel("Latency per image (ms)")
    plt.plot(bs_list, lat_img, marker="o")
    plt.grid(True, linestyle="--", alpha=0.5)
    p1 = out / "latency_per_image_vs_batch.png"
    plt.savefig(p1, dpi=160, bbox_inches="tight"); print(f"Saved: {p1}")

    plt.figure()
    plt.title(f"YOLO throughput vs batch (device={device}, half={use_half})")
    plt.xlabel("Batch size"); plt.ylabel("Images per second")
    plt.plot(bs_list, thrput, marker="o")
    plt.grid(True, linestyle="--", alpha=0.5)
    p2 = out / "throughput_vs_batch.png"
    plt.savefig(p2, dpi=160, bbox_inches="tight"); print(f"Saved: {p2}")

def parse_args():
    ap = argparse.ArgumentParser("Ultralytics YOLO batch benchmarking")
    ap.add_argument("--model", type=str, default="E:/distributed-dds-ai-serving-system/inference_backend/src/yolo_parameter/best.pt")
    ap.add_argument("--source", type=str, default="E:/distributed-dds-ai-serving-system/inference_backend/src/workdir/task-test-1.jpg", help="单张图片或图片文件夹（将重复使用以凑满批次）")
    ap.add_argument("--out_dir", type=str, default="./yolo_bench_out")
    ap.add_argument("--batches", type=int, nargs="+", default=[1,2,4,8,16,32,64], help="批大小列表")
    ap.add_argument("--iters", type=int, default=50, help="每个批大小下的批次数（计时迭代）")
    ap.add_argument("--warmup", type=int, default=10, help="预热批次数")
    ap.add_argument("--imgsz", type=int, default=640, help="推理尺寸")
    ap.add_argument("--device", type=str, default="cuda", choices=["cuda","cpu"])
    ap.add_argument("--half", action="store_true", help="CUDA 上使用 FP16（自动混合精度）")
    ap.add_argument("--conf", type=float, default=0.25)
    ap.add_argument("--iou",  type=float, default=0.45)
    ap.add_argument("--max_det", type=int, default=300)
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    # 提高 cuDNN 性能（对定形输入有效）
    torch.backends.cudnn.benchmark = True
    run_bench(
        model_path=args.model,
        source_path=args.source,
        out_dir=args.out_dir,
        batches=list(range(1, 17)),
        iters=args.iters,
        warmup=args.warmup,
        imgsz=args.imgsz,
        device=args.device,
        half=args.half,
        conf=args.conf,
        iou=args.iou,
        max_det=args.max_det,
        verbose=args.verbose
    )
