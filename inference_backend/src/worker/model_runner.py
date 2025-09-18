#!/usr/bin/env python3
"""
ModelRunner - Python version of the Java ModelRunner class
(仅通过 TaskRunner 调用具体 runner；OCR/YOLO 均不落盘输入，直接用 DDS 字节)
"""

import base64
import io
import shutil
import time
import uuid
from pathlib import Path
from typing import Optional, NamedTuple, List, Tuple, Callable, Any, Dict
from copy import deepcopy

import DDS_All
from DDS_All import TaskList, WorkerResult, WorkerTaskResult
import logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"

# 2. 配置 logging
logging.basicConfig(
    level=logging.INFO,        # DEBUG < INFO < WARNING < ERROR < CRITICAL
    format=log_format
)

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
          "model_id": "yolo" | "ocr" | ...,
          "model_config": {
            "parameter": "...",            # 必填：权重(文件或目录)
            "params": {...}                # 可选（如 yolo 的 conf/iou）
          }
        }
        """
        if "model_id" not in base_config or "model_config" not in base_config:
            raise KeyError("base_config must contain 'model_id' and 'model_config'")

        mc = base_config["model_config"]
        # 兼容旧字段
        parameter = mc.get("parameter") or mc.get("model_parameter")
        if not parameter:
            raise KeyError("base_config.model_config.parameter (or model_parameter) is required")

        self.base_model_id = base_config["model_id"]
        self.base_model_config = dict(mc)
        self.base_model_config["parameter"] = parameter
        self.base_model_config["model_parameter"] = parameter
        self.model_config: Dict[str, Any] = deepcopy(base_config["model_config"])

    # ====== 入口：按批运行（DDS 传入的 TaskList）======
    def run_batched_task(self, tasks: TaskList) -> WorkerResult:
        wr = WorkerResult()
        wr.batch_id = tasks.batch_id
        wr.model_id = tasks.model_id

        logging.info("[WorkerRunner] batch_id: %s model_id: %s", wr.batch_id, wr.model_id)

        seq = getattr(tasks, "tasks", None) or []
        n, get_at = self._make_seq_accessors(seq)

        # 统一用 Python list
        results_list: List[WorkerTaskResult] = []
        if n == 0:
            wr.results = results_list
            return wr

        run_paths: Optional[RunPaths] = None
        try:
            # 临时工作目录
            run_paths = self._make_batch_run_paths(tasks.batch_id)

            # 1) 组装内存批处理 images: [(task_id, bytes), ...]
            images: List[Tuple[str, bytes]] = []
            for i in range(n):
                t = get_at(i)
                tid = str(getattr(t, "task_id", f"task_{i}"))
                payload = getattr(t, "payload", None)
                try:
                    img_bytes = self._coerce_to_bytes(payload)
                except Exception:
                    img_bytes = b""
                images.append((tid, img_bytes))

            # 2) 构造本批 model_config（内存 batch）
            model_id = (tasks.model_id or self.base_model_id).lower()
            model_config = self._build_model_config_for_batch(
                model_id=model_id,
                output_dir=run_paths.outputs,
                images=images
            )
            config_obj = {"model_id": model_id, "model_config": model_config}

            # 执行具体 runner（兼容两种路径）
            try:
                from model_service.task_runner import TaskRunner 
            except ImportError:
                from task_runner import TaskRunner                # 若 task_runner.py 在项目根
            TaskRunner(config_obj).execute()

            # 3) 回读输出，填充结果（统一 bytes；OCR 额外回 texts）
            for i in range(n):
                t = get_at(i)
                res = WorkerTaskResult()
                res.request_id = getattr(t, "request_id", "")
                res.task_id = getattr(t, "task_id", "")
                res.client_id = getattr(t, "client_id", "")

                # 3.1 output_blob：优先读取图片/常见格式
                data = self._read_first_output_of_task(run_paths.outputs, str(res.task_id))
                if data is not None:
                    res.status = "OK"
                    res.output_blob = data
                else:
                    res.status = "ERROR_NO_OUTPUT"
                    res.output_blob = b""

                # 3.2 texts：仅 OCR 读取 <task_id>.txt；YOLO/其他为空列表
                if model_id in {"ocr", "rapidocr", "textocr"}:
                    texts = self._read_texts_of_task(run_paths.outputs, str(res.task_id))
                    self._set_texts(res, texts)   # 非空则 Spliter 会下发；空等价于无
                else:
                    self._set_texts(res, [])      # YOLO 文本置空即可

                results_list.append(res)

            wr.results = results_list
            return wr

        except Exception as e:
            logging.error("Batch processing error: %s", e)
            # 兜底：构造 ERROR_RUNNER
            for i in range(n):
                t = get_at(i)
                res = WorkerTaskResult()
                res.request_id = getattr(t, "request_id", "")
                res.task_id = getattr(t, "task_id", "")
                res.client_id = getattr(t, "client_id", "")
                res.status = "ERROR_RUNNER"
                res.output_blob = b""
                self._set_texts(res, [])  # 兜底时不发文本
                results_list.append(res)
            wr.results = results_list
            return wr
        finally:
            if run_paths:
                self._delete_recursive_quietly(run_paths.root)

    # ====== 动态拼接本批的 model_config ======
    def _build_model_config_for_batch(self, model_id: str, output_dir: Path,
                                      images: List[Tuple[str, bytes]]) -> dict:
        cfg = dict(self.base_model_config)
        cfg["batch"] = True
        cfg["images"] = images                  # [(task_id, bytes), ...]
        cfg["output"] = str(output_dir)
        cfg["output_names"] = [task_id for task_id, _ in images]
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
    def _delete_recursive_quietly(root_path: Optional[Path]) -> None:
        if not root_path or not root_path.exists():
            return
        try:
            shutil.rmtree(root_path, ignore_errors=True)
        except Exception as e:
            logging.error("Failed to delete %s: %s", root_path, e)

    @staticmethod
    def _safe_name(name: str, default: str = "unknown") -> str:
        if not name:
            return default
        safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
        return safe or default

    # ====== 统一访问 DDS Seq / Python list ======
    @staticmethod
    def _make_seq_accessors(seq: Any) -> Tuple[int, Callable[[int], Any]]:
        try:
            n = seq.length()
            getter = seq.get_at
            return n, getter
        except AttributeError:
            n = len(seq)
            return n, (lambda i: seq[i])

    # ====== 将任意 payload 转 bytes（绝不对图片做 UTF-8 解码）======
    @staticmethod
    def _coerce_to_bytes(blob: Any) -> bytes:
        if blob is None:
            return b""

        if isinstance(blob, (bytes, bytearray, memoryview)):
            return bytes(blob)

        if isinstance(blob, str):
            # 尝试 base64
            try:
                return base64.b64decode(blob, validate=True)
            except Exception:
                p = Path(blob)
                if p.exists() and p.is_file():
                    return p.read_bytes()
                return b""

        # DDS ByteSeq（length/get_at）
        try:
            n = blob.length()
            get_at = blob.get_at
            return bytes(bytearray(get_at(i) for i in range(n)))
        except Exception:
            pass

        # 最后尝试 buffer 协议
        try:
            return bytes(blob)
        except Exception:
            return b""

    # ====== 回读输出（用于 output_blob）======
    @staticmethod
    def _read_first_output_of_task(output_dir: Path, task_id: str) -> Optional[bytes]:
        if not output_dir.exists():
            return None
        preferred_exts = ["jpg", "jpeg", "png", "webp", "json", "txt", "bin"]
        for ext in preferred_exts:
            p = output_dir / f"{task_id}.{ext}"
            if p.exists() and p.is_file():
                return p.read_bytes()
        candidates = list(output_dir.glob(f"{task_id}.*"))
        if not candidates:
            p_plain = output_dir / f"{task_id}"
            if p_plain.exists() and p_plain.is_file():
                return p_plain.read_bytes()
            return None
        def _rank(path: Path):
            ext = path.suffix.lower().lstrip(".")
            rank = preferred_exts.index(ext) if ext in preferred_exts else len(preferred_exts) + 1
            return (rank, -path.stat().st_mtime)
        best = sorted(candidates, key=_rank)[0]
        return best.read_bytes() if best.exists() else None

    # ====== 回读 OCR 文本（<task_id>.txt，每行一条；忽略空行）======
    @staticmethod
    def _read_texts_of_task(output_dir: Path, task_id: str) -> List[str]:
        txt = output_dir / f"{task_id}.txt"
        if not txt.exists() or not txt.is_file():
            return []
        try:
            # 尝试 UTF-8；失败再退回默认编码
            try:
                content = txt.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                content = txt.read_text()
            lines = [ln.strip() for ln in content.splitlines()]
            return [ln for ln in lines if ln]
        except Exception as e:
            logging.warning("read texts failed for %s: %s", txt, e)
            return []

    # ====== 写入 WorkerTaskResult.texts（本绑定直接用 list[str]）======
    @staticmethod
    def _set_texts(res: WorkerTaskResult, texts: List[str]) -> None:
        # 直接赋值即可；C++ 侧会 ensure_length + set_at 完成拷贝。
        res.texts = [str(s) for s in texts]
