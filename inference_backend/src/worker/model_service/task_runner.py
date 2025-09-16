import json, os, sys, importlib, argparse
from typing import Union, Mapping
sys.path.append(os.path.dirname(__file__))
import logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=log_format
)

class TaskRunner:
    def __init__(self, config_or_path: Union[str, Mapping]):
        if isinstance(config_or_path, str):
            with open(config_or_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        elif isinstance(config_or_path, Mapping):
            config = dict(config_or_path)
        else:
            raise TypeError("config_or_path must be a path(str) or dict.")

        if "model_id" not in config or "model_config" not in config:
            raise KeyError("config must contain 'model_id' and 'model_config'")
        if "parameter" not in config["model_config"]:
            raise KeyError("model_config must contain 'parameter'")

        self.model_id = config["model_id"]
        # 传给具体 runner（包含 parameter / input / output / params / batch / images ）
        self.model_config = dict(config["model_config"])

    def _import_runner(self, model_id: str):
        name = f'runners.{model_id}_runner'
        return importlib.import_module(name)

    def execute(self):
        logging.info(f"\n=== 开始任务: %s ===",self.model_id)
        module = self._import_runner(self.model_id)
        func = getattr(module, f"run_{self.model_id}", None)
        if not callable(func):
            raise AttributeError(f"{module.__name__}.run_{self.model_id} not found")
        func(self.model_config)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generic Task Runner")
    p.add_argument("--config_file", required=True, help="path to config json")
    args = p.parse_args()
    if not os.path.exists(args.config_file):
        logging.error(f"not found: %s",args.config_file); sys.exit(1)
    TaskRunner(args.config_file).execute()
