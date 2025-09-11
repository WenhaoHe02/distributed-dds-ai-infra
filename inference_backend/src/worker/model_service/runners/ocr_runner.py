from rapidocr import RapidOCR
from pathlib import Path
from typing import List

def resolve_model_path(path, suffix=".onnx", return_dir=False):
    p = Path(path)
    if p.is_file():
        return p.parent if return_dir else p
    if p.is_dir():
        if return_dir:
            return p
        cands = list(p.glob(f"*{suffix}"))
        if not cands:
            raise FileNotFoundError(f"目录中未找到模型文件（{suffix}）：{p}")
        if len(cands) > 1:
            raise ValueError(f"目录中有多个模型文件（不确定加载哪个）：{cands}")
        return cands[0]
    raise FileNotFoundError(f"路径无效：{p}")

def _save_ocr_outputs(result, out_dir: Path, stem: str):
    """统一输出：<stem>.jpg（可视化），<stem>.txt（文本）"""
    out_dir.mkdir(parents=True, exist_ok=True)
    vis_path = out_dir / f"{stem}.jpg"
    txt_path = out_dir / f"{stem}.txt"

    if hasattr(result, "vis"):
        result.vis(str(vis_path))
    if hasattr(result, "txts"):
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(result.txts))

def run_ocr(task_config: dict):
    """
    统一入口（由 TaskRunner 调用）
    task_config 需要：
      parameter: OCR 模型目录或文件（目录下含 det.onnx / rec.onnx / 可选 cls.onnx）
      output: 输出目录

      普通模式（二选一之一）：
        input: 输入目录或单文件

      批处理模式（二选一之一）：
        batch: true
        input_files: [str, ...]         # 每个元素为图片路径
        output_names: [str, ...]        # 与 input_files 对应的输出名（如 task_id）
    """
    # 1) 解析模型
    model_dir = resolve_model_path(task_config["parameter"], suffix=".onnx", return_dir=True)
    det_model = model_dir / "det.onnx"
    rec_model = model_dir / "rec.onnx"
    cls_model = model_dir / "cls.onnx"  # 可选

    if not det_model.exists() or not rec_model.exists():
        raise FileNotFoundError(f"缺少 OCR 模型文件：{det_model} 或 {rec_model}")

    engine = RapidOCR(
        det_model_path=str(det_model),
        rec_model_path=str(rec_model),
        cls_model_path=str(cls_model) if cls_model.exists() else None
    )

    out_dir = Path(task_config["output"])
    out_dir.mkdir(parents=True, exist_ok=True)

    # 2) 批处理模式（供 ModelRunner 批量调用）
    if task_config.get("batch") and task_config.get("input_files"):
        input_files: List[str] = task_config["input_files"]
        names: List[str] = task_config.get("output_names") or [Path(p).stem for p in input_files]
        if len(names) != len(input_files):
            raise ValueError("output_names 长度需与 input_files 一致")
        for name, img in zip(names, input_files):
            try:
                result = engine(str(img))
                _save_ocr_outputs(result, out_dir, stem=str(name))
            except Exception as e:
                print(f"[ERROR] 图像 {img} 处理失败: {e}")
        print(f"[INFO] OCR batch 完成，输出：{out_dir}")
        return

    # 3) 普通目录/单文件模式
    input_path = Path(task_config["input"])
    if not input_path.exists():
        print(f"[ERROR] 输入路径不存在: {input_path}")
        return

    files = [input_path] if input_path.is_file() else sorted(input_path.glob("*.*"))
    if not files:
        print(f"[WARNING] 输入目录中未找到图像文件: {input_path}")
        return

    for img_path in files:
        try:
            result = engine(str(img_path))
            stem = img_path.stem  # 与输入同名，便于 ModelRunner 回读 <task_id>.jpg
            _save_ocr_outputs(result, out_dir, stem=stem)
        except Exception as e:
            print(f"[ERROR] 图像 {img_path} 处理失败: {e}")

    print(f"[INFO] OCR 完成，输出：{out_dir}")
