import argparse
import os
from ultralytics import YOLO
from PIL import Image
from io import BytesIO
import base64

def is_base64(s):
    try:
        return base64.b64encode(base64.b64decode(s)) == s.encode()
    except Exception:
        return False

def decode_base64_to_image(b64_str):
    try:
        byte_data = base64.b64decode(b64_str)
        img = Image.open(BytesIO(byte_data)).convert("RGB")
        return img
    except Exception as e:

        return None

def get_image_list(path_or_file):
    if os.path.isdir(path_or_file):
        # 如果是目录，就返回目录下所有图片路径
        supported_ext = (".jpg", ".jpeg", ".png", ".bmp", ".webp")
        return [os.path.join(path_or_file, f) for f in os.listdir(path_or_file) if f.lower().endswith(supported_ext)]
    elif os.path.isfile(path_or_file):
        return [path_or_file]
    else:
        return []

def main():
    parser = argparse.ArgumentParser(description="YOLO 图像推理")
    parser.add_argument("--path", type=str, required=True)
    parser.add_argument("--task_id", type=str, required=True)
    args = parser.parse_args()

    # 使用动态路径查找模型文件
    model_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "yolo_parameter", "best.pt")
    model = YOLO(model_path)
    task_id = args.task_id
    if is_base64(args.path):

        img = decode_base64_to_image(args.path)
        if img:
            results = model(img)
        else:

            return
    else:
        # 支持目录或单图像路径
        inputs = get_image_list(args.path)
        if not inputs:

            return
        results = model(inputs)  # 自动打 batch

    # 输出与保存结果
    save_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        f"{task_id}.jpg"
    )
    for result in results:
        result.save(filename=save_path)  # 保存预测图


if __name__ == "__main__":
    main()