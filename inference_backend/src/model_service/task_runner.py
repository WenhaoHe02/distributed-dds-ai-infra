import yaml
import importlib

class TaskRunner:
    def __init__(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

    def execute_all(self):
        for task_name, config in self.config['tasks'].items():
            print(f"\n=== 开始任务: {task_name} ===")
            model_ids = [mid.strip() for mid in config['model_id'].split(',')]
            for mid in model_ids:
                # 组合任务特殊处理
                if mid == "yolo+ocr":
                    module_name = "runners.combined_runner"
                    func_name = "run_combined"
                else:
                    module_name = f"runners.{mid}_runner"
                    func_name = f"run_{mid}"
                try:
                    module = importlib.import_module(module_name)
                    func = getattr(module, func_name)
                    func(config)
                except (ModuleNotFoundError, AttributeError) as e:
                    print(f"无法调用模型 {mid}: {e}")

if __name__ == "__main__":
    runner = TaskRunner("config.yaml")
    runner.execute_all()

  