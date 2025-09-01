import argparse
import os
from ultralytics import YOLO
from PIL import Image
from io import BytesIO
import base64

def is_base64(s: str) -> bool:
    try:
        return base64.b64encode(base64.b64decode(s)) == s.encode()
    except Exception:
        return False

def decode_base64_to_image(b64_str):
    try:
        byte_data = base64.b64decode(b64_str)
        img = Image.open(BytesIO(byte_data)).convert("RGB")
        return img
    except Exception:
        return None

def get_image_list(path_or_file):
    if os.path.isdir(path_or_file):
        supported_ext = (".jpg", ".jpeg", ".png", ".bmp", ".webp")
        return [os.path.join(path_or_file, f)
                for f in os.listdir(path_or_file)
                if f.lower().endswith(supported_ext)]
    elif os.path.isfile(path_or_file):
        return [path_or_file]
    else:
        return []

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="YOLO 图像推理（支持批处理）")
    parser.add_argument("--path", type=str, required=True, help="单文件、目录路径，或 base64 字符串")
    parser.add_argument("--task_id", type=str, default=None, help="单文件/内存模式下用于命名输出")
    parser.add_argument("--out_dir", type=str, required=True, help="输出目录")
    args = parser.parse_args()

    model_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              "yolo_parameter", "best.pt")
    model = YOLO(model_path)
    ensure_dir(args.out_dir)

    # Base64 输入
    if is_base64(args.path):
        img = decode_base64_to_image(args.path)
        if not img:
            print("ERROR: invalid base64")
            return
        results = model(img)
        filename = (args.task_id + ".jpg") if args.task_id else "output.jpg"
        save_path = os.path.join(args.out_dir, filename)
        for r in results:
            r.save(filename=save_path)
        print(f"SAVED: {save_path}")
        return

    # 文件或目录输入
    inputs = get_image_list(args.path)
    if not inputs:
        print("ERROR: no valid inputs")
        return

    results = model(inputs)

    if os.path.isdir(args.path):
        for r in results:
            src_path = getattr(r, "path", None) or getattr(r, "path_orig", None)
            if src_path is None:
                idx = results.index(r)
                src_path = inputs[idx]
            basename = os.path.basename(src_path)
            save_path = os.path.join(args.out_dir, basename)
            r.save(filename=save_path)
            print(f"SAVED: {save_path}")
    else:
        src = inputs[0]
        filename = (args.task_id + ".jpg") if args.task_id else os.path.basename(src)
        save_path = os.path.join(args.out_dir, filename)
        for r in results:
            r.save(filename=save_path)
        print(f"SAVED: {save_path}")

if __name__ == "__main__":
    main()
