from rapidocr import RapidOCR
from pathlib import Path
import os

def run_ocr(task_config):
    """执行OCR文本识别任务"""
    engine = RapidOCR()
    input_path = Path(task_config['input'])
    output_dir = Path(task_config['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    for img_path in input_path.glob('*.*'):
        result = engine(str(img_path))
        output_file = output_dir / f"{img_path.stem}_result.txt"
        jpg_path = os.path.join(output_dir, f"{img_path.stem}_ocr_result.jpg")
        result.vis(jpg_path)
        print(f"可视化结果已保存到: {jpg_path}")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(result.txts))