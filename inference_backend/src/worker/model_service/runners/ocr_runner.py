# ocr_runner.py
from __future__ import annotations
from rapidocr import RapidOCR
from pathlib import Path
from typing import List, Tuple, Union, Dict, Any
from typing import List, Tuple, Union
from PIL import Image
import io
import logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"

# 2. 配置 logging
logging.basicConfig(
    level=logging.INFO,        # 设置日志级别：DEBUG < INFO < WARNING < ERROR < CRITICAL
    format=log_format          # 设置输出格式
)
# -----------------------
# Utils
# -----------------------

def resolve_model_path(path, suffix: str = ".onnx", return_dir: bool = False) -> Path:
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

def _save_ocr_outputs(result: Any, out_dir: Path, stem: str):
    """
    统一输出：<stem>.jpg（可视化，若前面没保存过则尝试）、<stem>.txt（文本）
    - 文本通过 _parse_texts(result) 解析，保证 join 的是可迭代
    - 如果没有文本，写入空文件（或按需改成不落盘）
    - 如果前面已经保存过可视化图，这里不再重复保存
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    vis_path = out_dir / f"{stem}.jpg"
    txt_path = out_dir / f"{stem}.txt"

    # —— 可视化图：若之前没保存过，这里尝试一次 —— #
    if not vis_path.exists():
        vis = getattr(result, "vis", None)
        if callable(vis):
            try:
                vis(str(vis_path))
            except Exception:
                # 不影响文本输出
                pass

    texts = _parse_texts(result)  # 一定返回 list[str] 或 []
    try:
        with open(txt_path, "w", encoding="utf-8") as f:
            if texts:
                f.write("\n".join(map(str, texts)))
            else:
                # 没有文本就写空，避免 join(None) 报错
                f.write("")
    except Exception as e:
        logging.error("写入文本失败: %s -> %s", txt_path, e)

def _to_pil(img: Union[bytes, bytearray, memoryview, Image.Image, str, Path]) -> Image.Image:

    """将多种输入转换为 PIL.Image（支持 bytes / 文件路径 / PIL.Image）"""
    if isinstance(img, Image.Image):
        return img.convert("RGB")
    if isinstance(img, (bytes, bytearray, memoryview)):
        return Image.open(io.BytesIO(bytes(img))).convert("RGB")
    # 字符串或 Path 视为文件路径
    p = Path(img)
    return Image.open(p).convert("RGB")

def _parse_texts(result: Any) -> List[str]:
    """
    抽取识别文本为 List[str]：
    - 对象风格：优先取 result.txts（通常为 list/tuple[str]）
    - 列表/元组风格：常见为 [(box, text, score), ...]，提取 text
    - 其余返回 []
    参考：RapidOCR 示例里有 result.vis(...) 与文本结果，常见输出包含文本列表。:contentReference[oaicite:1]{index=1}
    """
    # 1) 对象属性
    txts = getattr(result, "txts", None)
    if isinstance(txts, (list, tuple)):
        return [str(t) for t in txts]

    # 2) 三元组列表形式：[(box, text, score), ...]
    if isinstance(result, (list, tuple)) and len(result) >= 1:
        # 有些返回形式为 (res, elapse)
        items = result[0] if isinstance(result[0], (list, tuple)) else result
        texts: List[str] = []
        try:
            for it in items or []:
                if isinstance(it, (list, tuple)) and len(it) >= 2:
                    texts.append(str(it[1]))
        except Exception:
            pass
        return texts

    return []

def _save_vis_or_original(result: Any, out_dir: Path, stem: str, source_img: Image.Image):
    """
    只产出图片：优先保存可视化图（带框）；若无法可视化或检测为空则保存原图占位。
    RapidOCR 的结果对象通常提供 result.vis(path) 来落地可视化图。:contentReference[oaicite:2]{index=2}
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    vis_path = out_dir / f"{stem}.jpg"

    saved = False
    vis = getattr(result, "vis", None)
    if callable(vis):
        try:
            vis(str(vis_path))
            if vis_path.exists() and vis_path.stat().st_size > 0:
                saved = True
        except Exception:
            saved = False

    if not saved:
        try:
            source_img.save(vis_path, format="JPEG")
        except Exception as e:
            logging.error(f"保存占位图失败: %s",e)

# -----------------------
# Runner
# -----------------------
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

    # RapidOCR 支持通过 params（以及 config_path）注入引擎与模型；详见 PyPI/示例。:contentReference[oaicite:3]{index=3}

    engine = RapidOCR(params=params)

    out_dir = Path(task_config["output"])
    out_dir.mkdir(parents=True, exist_ok=True)

    texts_map: Dict[str, List[str]] = {}

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
                # 文本
                texts_map[stem] = _parse_texts(result)
                # 图片（可视化或原图）
                _save_vis_or_original(result, out_dir, stem=stem, source_img=img)
                _save_ocr_outputs(result, out_dir, stem=stem)
            except Exception as e:
                logging.error(f"task %s 处理失败: %s",task_id,e)
                texts_map[stem] = []
        logging.info(f"OCR batch 完成（内存模式），输出：%s",out_dir)
        return texts_map

    # 3) 兼容旧的目录/单文件模式（需要提前把 DDS 字节落盘到 input 目录）
    input_path = Path(task_config["input"])
    if not input_path.exists():
        logging.error(f"输入路径不存在: %s",input_path)
        return texts_map

    files = [input_path] if input_path.is_file() else sorted(input_path.glob("*.*"))
    if not files:
        logging.warning(f"输入目录中未找到图像文件: %s",input_path)
        return texts_map

    for img_path in files:
        stem = img_path.stem
        try:
            img = _to_pil(img_path)
            result = engine(img)
            texts_map[stem] = _parse_texts(result)
            _save_vis_or_original(result, out_dir, stem=stem, source_img=img)
            stem = img_path.stem
            _save_ocr_outputs(result, out_dir, stem=stem)
        except Exception as e:
            logging.error(f"图像 %s 处理失败: %s",img_path,e)
            texts_map[stem] = []

    logging.info(f"OCR 完成（目录模式），输出：%s",out_dir)
    return texts_map
