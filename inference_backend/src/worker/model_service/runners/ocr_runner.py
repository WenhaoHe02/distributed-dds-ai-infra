from rapidocr import RapidOCR
from pathlib import Path
from typing import List, Tuple, Union
from PIL import Image
import on
import io

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
    # RapidOCR 的结果对象通常提供 vis()/txts
    if hasattr(result, "vis"):
        result.vis(str(vis_path))
    if hasattr(result, "txts"):
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(result.txts))

def _to_pil(img: Union[bytes, bytearray, memoryview, Image.Image, str, Path]) -> Image.Image:
    """将多种输入转换为 PIL.Image（支持 bytes / 文件路径 / PIL.Image）"""
    if isinstance(img, Image.Image):
        return img.convert("RGB")
    if isinstance(img, (bytes, bytearray, memoryview)):
        return Image.open(io.BytesIO(bytes(img))).convert("RGB")
    # 字符串或 Path 视为文件路径
    p = Path(img)
    return Image.open(p).convert("RGB")

def run_ocr(task_config: dict):
    """
    统一入口（由 TaskRunner 调用）
    task_config 至少包含：
      parameter: OCR 模型目录或文件（目录下含 det.onnx / rec.onnx / 可选 cls.onnx）
      output: 输出目录

    支持两种数据来源（二选一）：
      A) 目录/单文件模式：
         input: 输入目录或单文件路径
      B) 内存批处理模式（推荐用于 DDS）：
         batch: true
         images: List[Tuple[task_id, bytes]] 或 List[dict{task_id, bytes}]
                 也兼容 List[Tuple[task_id, file_path]]
         （可选）output_names: 与 images 对应的输出名；不提供则使用 task_id
    """
    # 1) 解析模型
    model_dir = resolve_model_path(task_config["parameter"], suffix=".onnx", return_dir=True)
    det_model = model_dir / "det.onnx"
    rec_model = model_dir / "rec.onnx"
    cls_model = model_dir / "cls.onnx"  # 可选

    if not det_model.exists() or not rec_model.exists():
        raise FileNotFoundError(f"缺少 OCR 模型文件：{det_model} 或 {rec_model}")

    params = {
        "Det.model_path": det_model,
        "Rec.model_path": rec_model,
        "Cls.model_path": cls_model if Path(cls_model).exists() else "",
        "Global.use_cls": Path(cls_model).exists(),
        "Global.log_level": "INFO"
    }
    engine = RapidOCR(params=params)

    out_dir = Path(task_config["output"])
    out_dir.mkdir(parents=True, exist_ok=True)

    # 2) ✅ 内存批处理（直接接 DDS 字节）
    if task_config.get("batch") and "images" in task_config:
        raw_images = task_config["images"]
        # 允许两种格式：[(task_id, bytes), ...] 或 [{"task_id":..., "bytes":...}, ...]
        images: List[Tuple[str, Union[bytes, bytearray, memoryview, str, Path, Image.Image]]] = []

        for item in raw_images:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                task_id, buff = item[0], item[1]
            elif isinstance(item, dict) and "task_id" in item and ("bytes" in item or "image" in item):
                task_id = item["task_id"]
                buff = item.get("bytes", item.get("image"))
            else:
                raise ValueError("images 元素需为 (task_id, bytes) 或 {'task_id':..., 'bytes':...}")

            images.append((str(task_id), buff))

        names = task_config.get("output_names")
        if names and len(names) != len(images):
            raise ValueError("output_names 长度需与 images 一致")

        for idx, (task_id, buff) in enumerate(images):
            stem = names[idx] if names else task_id
            try:
                img = _to_pil(buff)
                result = engine(img)
                _save_ocr_outputs(result, out_dir, stem=stem)
            except Exception as e:
                print(f"[ERROR] task {task_id} 处理失败: {e}")

        print(f"[INFO] OCR batch 完成（内存模式），输出：{out_dir}")
        return

    # 3) 兼容旧的目录/单文件模式（需要提前把 DDS 字节落盘到 input 目录）
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
            img = _to_pil(img_path)
            result = engine(img)
            stem = img_path.stem
            _save_ocr_outputs(result, out_dir, stem=stem)
        except Exception as e:
            print(f"[ERROR] 图像 {img_path} 处理失败: {e}")

    print(f"[INFO] OCR 完成（目录模式），输出：{out_dir}")
