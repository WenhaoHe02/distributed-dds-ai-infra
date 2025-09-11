#!/usr/bin/env python3
"""
ModelRunner - Python version of the Java ModelRunner class
(从 worker 读取基础配置；仅通过 TaskRunner 调用具体 runner)
"""

import shutil, time, uuid
from pathlib import Path
from typing import Optional, NamedTuple

from DDS_All import TaskList, WorkerResult, WorkerTaskResult, WorkerTaskResultSeq

class RunPaths(NamedTuple):
    run_id: str
    root: Path
    inputs: Path
    outputs: Path

class ModelRunner:
    """Python ModelRunner implementation"""

    def __init__(self, base_config: dict):
        """
        base_config 形如：
        {
          "model_id": "yolo",
          "model_config": {
            "parameter": "...",            # 必填
            "params": {...}                # 可选（比如 yolo 的 conf/iou）
          }
        }
        """
        if "model_id" not in base_config or "model_config" not in base_config:
            raise KeyError("base_config must contain 'model_id' and 'model_config'")
        if "parameter" not in base_config["model_config"]:
            raise KeyError("base_config.model_config.parameter is required")

        self.base_model_id = base_config["model_id"]
        # 拷贝一份作为模板，后面每个 batch 在此基础上补充 input/output/batch 等字段
        self.base_model_config = dict(base_config["model_config"])

    # ====== 入口：按批运行（DDS 传入的 TaskList）======
    def run_batched_task(self, tasks: TaskList) -> WorkerResult:
        wr = WorkerResult()
        wr.batch_id = tasks.batch_id
        wr.model_id = tasks.model_id

        print("[WorkerRunner] batch_id:", wr.batch_id, " model_id:", wr.model_id)
        n = len(tasks.tasks) if tasks.tasks else 0

        results_seq = WorkerTaskResultSeq()
        results_seq.ensure_length(n, n)
        if n == 0:
            wr.results = results_seq
            return wr

        run_paths = None
        try:
            run_paths = self._make_batch_run_paths(tasks.batch_id)

            # 1) 写 inputs/<task_id>.jpg
            for i in range(n):
                t = tasks.tasks[i]
                if not t or not t.payload: continue
                self._atomic_write(run_paths.inputs / f"{t.task_id}.jpg", bytes(t.payload))

            # 2) 构造本批 model_config，并通过 TaskRunner 调用对应 runner
            model_config = self._build_model_config_for_batch(
                model_id=tasks.model_id,
                input_dir=run_paths.inputs,
                output_dir=run_paths.outputs,
                task_ids=[t.task_id for t in tasks.tasks]
            )
            config_obj = {"model_id": tasks.model_id, "model_config": model_config}

            # 只调用 TaskRunner（不直接 import 具体 runner）
            try:
                from model_service.runners.task_runner import TaskRunner
            except ModuleNotFoundError:
                from worker.model_service.runners.task_runner import TaskRunner
            TaskRunner(config_obj).execute()

            # 3) 回读 outputs/<task_id>.jpg
            for i in range(n):
                t = tasks.tasks[i]
                res = WorkerTaskResult()
                res.request_id = t.request_id
                res.task_id = t.task_id
                res.client_id = t.client_id

                out_path = run_paths.outputs / f"{t.task_id}.jpg"
                if out_path.exists():
                    res.status = "OK"
                    res.output_blob = bytes(out_path.read_bytes())
                else:
                    res.status = "ERROR_NO_OUTPUT"
                    res.output_blob = b""
                results_seq.set_at(i, res)

            wr.results = results_seq
            return wr

        except Exception as e:
            print(f"Batch processing error: {e}")
            for i in range(n):
                t = tasks.tasks[i]
                res = WorkerTaskResult()
                res.request_id = t.request_id
                res.task_id = t.task_id
                res.client_id = t.client_id
                res.status = "ERROR"
                res.output_blob = b""
                results_seq.set_at(i, res)
            wr.results = results_seq
            return wr
        finally:
            if run_paths:
                self._delete_recursive_quietly(run_paths.root)

    # ====== 把 batch 相关字段补到 model_config（只改这些动态字段）======
    def _build_model_config_for_batch(self, model_id: str, input_dir: Path, output_dir: Path, task_ids):
        cfg = dict(self.base_model_config)  # 基于 worker 读到的模板复制
        cfg["output"] = str(output_dir)

        if model_id == "yolo":
            # YOLO 真·batch：一次推完，并用 task_id 命名输出
            cfg.update({
                "batch": True,
                "input_files": [str(input_dir / f"{tid}.jpg") for tid in task_ids],
                "output_names": [str(tid) for tid in task_ids],
            })
        else:
            # 其它模型（如 OCR）先走目录模式（runner 内部逐张处理）
            cfg["input"] = str(input_dir)

        return cfg

    # ====== 路径与文件工具 ======
    @staticmethod
    def _make_batch_run_paths(batch_id: str) -> RunPaths:
        safe_batch = ModelRunner._safe_name(batch_id, "batch")
        run_id = f"{int(time.time()*1000)}_{str(uuid.uuid4())[:8]}"
        root = Path("workdir") / "batches" / safe_batch / run_id
        inputs, outputs = root / "inputs", root / "outputs"
        inputs.mkdir(parents=True, exist_ok=True)
        outputs.mkdir(parents=True, exist_ok=True)
        return RunPaths(run_id, root, inputs, outputs)

    @staticmethod
    def _atomic_write(target_path: Path, data: bytes) -> None:
        tmp = target_path.with_suffix(target_path.suffix + ".part")
        try:
            tmp.write_bytes(data)
            try:
                tmp.replace(target_path)
            except OSError:
                if target_path.exists(): target_path.unlink()
                tmp.rename(target_path)
        finally:
            if tmp.exists():
                try: tmp.unlink()
                except Exception: pass

    @staticmethod
    def _delete_recursive_quietly(root_path: Optional[Path]) -> None:
        if not root_path or not root_path.exists(): return
        try: shutil.rmtree(root_path, ignore_errors=True)
        except Exception as e: print(f"Failed to delete {root_path}: {e}")

    @staticmethod
    def _safe_name(name: str, default: str = "unknown") -> str:
        if not name: return default
        safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
        return safe or default
