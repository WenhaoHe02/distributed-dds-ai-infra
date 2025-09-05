from ultralytics import YOLO
from pathlib import Path

def run_yolo(task_config):
    """执行YOLO目标检测任务"""
    model_parameter = YOLO(task_config['model_parameter'])
    input_path = Path(task_config['input'])
    output_dir = Path(task_config['output'])
    output_dir.mkdir(parents=True, exist_ok=True)

    if input_path.is_dir():
        files = list(input_path.glob('*.*'))
    else:
        files = [input_path]

    for file in files:
        results = model_parameter.predict(
            source=str(file),
            conf=task_config.get('params', {}).get('conf', 0.25),
            iou=task_config.get('params', {}).get('iou', 0.45)
        )
        for result in results:
            save_yolo_results(result, output_dir)

def save_yolo_results(result, output_dir):
    result.save(filename=str(output_dir / f"{Path(result.path).stem}_vis.jpg"))
    '''
    with open(output_dir / f"{Path(result.path).stem}_boxes.txt", 'w') as f:
        for box, conf, cls in zip(result.boxes.xyxy, result.boxes.conf, result.boxes.cls):
            f.write(f"{result.names[int(cls)]} {conf:.2f} {box[0]:.0f} {box[1]:.0f} {box[2]:.0f} {box[3]:.0f}\n")
    '''