from ultralytics import YOLO
from pathlib import Path
from typing import List

def _resolve_weight(path, suffix=".pt"):
    p = Path(path)
    if p.is_file(): return p
    if p.is_dir():
        cands = list(p.glob(f"*{suffix}"))
        if not cands: raise FileNotFoundError(f"no weight {suffix} in {p}")
        if len(cands) > 1: raise ValueError(f"multi weights in {p}: {cands}")
        return cands[0]
    raise FileNotFoundError(f"invalid weight path: {p}")

def _save_result(res, out_dir: Path, stem: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    res.save(filename=str(out_dir / f"{stem}.jpg"))

def run_yolo(cfg: dict):
    """
    统一入口（由 TaskRunner 调用）
    cfg:
      parameter: 权重(.pt或目录)
      output: 输出目录
      params: {conf, iou} 可选
      # 二选一：
      input: 目录或文件（普通模式）
      或
      input_files: [str,...], output_names: [str,...], batch: true（批处理模式）
    """
    weight = _resolve_weight(cfg["parameter"])
    model = YOLO(str(weight))
    out_dir = Path(cfg["output"])
    conf = cfg.get("params", {}).get("conf", 0.25)
    iou  = cfg.get("params", {}).get("iou", 0.45)

    # 批处理模式：由 ModelRunner/TaskRunner 预先给定文件列表 + 输出名（与 task_id 一一对应）
    if cfg.get("batch") and cfg.get("input_files"):
        input_files: List[str] = cfg["input_files"]
        names: List[str] = cfg.get("output_names") or [Path(p).stem for p in input_files]
        results = model.predict(source=input_files, conf=conf, iou=iou, save=False, stream=False)
        for name, res in zip(names, results):
            _save_result(res, out_dir, stem=name)
        print(f"[INFO] YOLO batch 完成，输出：{out_dir}")
        return

    # 普通模式：目录或单文件
    in_path = Path(cfg["input"])
    files = [in_path] if in_path.is_file() else sorted(in_path.glob("*.*"))
    for f in files:
        results = model.predict(source=str(f), conf=conf, iou=iou, save=False)
        for res in results:
            _save_result(res, out_dir, stem=Path(res.path).stem if res.path else f.stem)
    print(f"[INFO] YOLO 完成，输出：{out_dir}")
