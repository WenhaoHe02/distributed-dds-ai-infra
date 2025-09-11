import json, os, sys, importlib, argparse
from typing import Union, Mapping

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
        self.model_config = dict(config["model_config"])  # 原样传 runner

    def _import_runner(self, model_id: str):
        for name in (f"{model_id}_runner",
                     f"runners.{model_id}_runner",
                     f"worker.model_service.runners.{model_id}_runner"):
            try:
                return importlib.import_module(name)
            except ModuleNotFoundError:
                continue
        raise ModuleNotFoundError(f"runner for '{model_id}' not found")

    def execute(self):
        print(f"\n=== 开始任务: {self.model_id} ===")
        module = self._import_runner(self.model_id)
        func = getattr(module, f"run_{self.model_id}", None)
        if not callable(func):
            raise AttributeError(f"{module.__name__}.run_{self.model_id} not found")
        func(self.model_config)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generic Task Runner")
    p.add_argument("--config_file", required=True)
    args = p.parse_args()
    if not os.path.exists(args.config_file):
        print(f"[ERROR] not found: {args.config_file}"); sys.exit(1)
    TaskRunner(args.config_file).execute()
