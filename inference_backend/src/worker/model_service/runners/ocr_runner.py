from rapidocr import RapidOCR
from pathlib import Path
from typing import List, Tuple, Union, Any
from PIL import Image
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

def _to_pil(img: Union[bytes, bytearray, memoryview, Image.Image, str, Path]) -> Image.Image:
    if isinstance(img, Image.Image):
        return img.convert("RGB")
    if isinstance(img, (bytes, bytearray, memoryview)):
        return Image.open(io.BytesIO(bytes(img))).convert("RGB")
    p = Path(img)
    return Image.open(p).convert("RGB")

def _save_vis_or_original(result: Any, out_dir: Path, stem: str, source_img: Image.Image):
    """
    只产出图片：优先保存可视化图（带框）；若无法可视化或检测为空，则保存原图占位。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    vis_path = out_dir / f"{stem}.jpg"

    saved = False
    vis = getattr(result, "vis", None)
    if callable(vis):
        try:
            vis(str(vis_path))     # RapidOCR 的可视化保存接口
            if vis_path.exists() and vis_path.stat().st_size > 0:
                saved = True
        except Exception:
            saved = False

    if not saved:
        try:
            # 回退：保存原图，确保一定有输出
            source_img.save(vis_path, format="JPEG")
            saved = True
        except Exception as e:
            print(f"[ERROR] 保存占位图失败: {e}")

def run_ocr(task_config: dict):
    """
    仅输出图片：
      - 内存批模式：task_config['batch']=True 且 task_config['images']=[(task_id, bytes|path|PIL), ...]
      - 目录/单文件模式：task_config['input'] 指向目录或文件
    模型参数：task_config['parameter'] 指向包含 det.onnx/rec.onnx（可选 cls.onnx）的目录
    输出：task_config['output'] 目录下生成 <task_id>.jpg
    """
    # 1) 模型装载（目录形式）
    model_dir = resolve_model_path(task_config["parameter"], suffix=".onnx", return_dir=True)
    det_model = model_dir / "det.onnx"
    rec_model = model_dir / "rec.onnx"
    cls_model = model_dir / "cls.onnx"

    if not det_model.exists() or not rec_model.exists():
        raise FileNotFoundError(f"缺少 OCR 模型文件：{det_model} 或 {rec_model}")

    params = {
        "Det.model_path": str(det_model),
        "Rec.model_path": str(rec_model),
        "Cls.model_path": str(cls_model) if cls_model.exists() else "",
        "Global.use_cls": cls_model.exists(),
        "Global.log_level": "INFO",
    }
    # 你的 RapidOCR 版本支持通过 params 覆盖配置
    engine = RapidOCR(params=params)

    out_dir = Path(task_config["output"])
    out_dir.mkdir(parents=True, exist_ok=True)

    # 2) 内存批处理（推荐）
    if task_config.get("batch") and "images" in task_config:
        raw_images = task_config["images"]
        images: List[Tuple[str, Union[bytes, bytearray, memoryview, str, Path, Image.Image]]] = []

        for item in raw_images:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                task_id, buff = item[0], item[1]
            elif isinstance(item, dict) and "task_id" in item and ("bytes" in item or "image" in item):
                task_id = item["task_id"]
                buff = item.get("bytes", item.get("image"))
            else:
                raise ValueError("images 元素需为 (task_id, bytes/path/PIL) 或 {'task_id':..., 'bytes'|'image':...}")
            images.append((str(task_id), buff))

        names = task_config.get("output_names")
        if names and len(names) != len(images):
            raise ValueError("output_names 长度需与 images 一致")

        for idx, (task_id, buff) in enumerate(images):
            stem = names[idx] if names else task_id
            try:
                img = _to_pil(buff)
                result = engine(img)
                _save_vis_or_original(result, out_dir, stem=stem, source_img=img)
            except Exception as e:
                print(f"[ERROR] task {task_id} 处理失败: {e}")

        print(f"[INFO] OCR batch 完成（内存模式），输出：{out_dir}")
        return

    # 3) 目录/单文件模式（兼容）
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
            _save_vis_or_original(result, out_dir, stem=img_path.stem, source_img=img)
        except Exception as e:
            print(f"[ERROR] 图像 {img_path} 处理失败: {e}")

    print(f"[INFO] OCR 完成（目录模式），输出：{out_dir}")
