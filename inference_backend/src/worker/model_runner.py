#!/usr/bin/env python3
"""
ModelRunner - Python version of the Java ModelRunner class
"""

import os
import sys
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional, NamedTuple
import platform

from DDS_All import (
    TaskList, WorkerResult, WorkerTaskResult, WorkerTaskResultSeq,
    Task
)


class RunPaths(NamedTuple):
    """Paths for a model run"""
    run_id: str
    root: Path
    inputs: Path
    outputs: Path


class ModelRunner:
    """Python ModelRunner implementation"""

    @staticmethod
    def run_batched_task(tasks: TaskList) -> WorkerResult:
        """Run a complete TaskList (true batch processing, clean up when done)"""
        worker_result = WorkerResult()
        worker_result.batch_id = tasks.batch_id
        worker_result.model_id = tasks.model_id

        n = tasks.tasks.length() if tasks and tasks.tasks else 0
        results_seq = WorkerTaskResultSeq()
        results_seq.ensure_length(n, n)

        if n == 0:
            worker_result.results = results_seq
            return worker_result

        run_paths = None
        try:
            run_paths = ModelRunner._make_batch_run_paths(tasks.batch_id)

            # 1) Write inputs/<task_id>.jpg
            for i in range(n):
                task = tasks.tasks.get_at(i)
                if not task or not task.payload or task.payload.length() == 0:
                    continue

                in_len = task.payload.length()
                in_bytes = bytearray(in_len)
                task.payload.to_array(in_bytes, in_len)

                input_path = run_paths.inputs / f"{task.task_id}.jpg"
                ModelRunner._atomic_write(input_path, bytes(in_bytes))

            # 2) Batch inference
            ModelRunner._run_model_batch(str(run_paths.inputs), str(run_paths.outputs))

            # 3) Read outputs
            for i in range(n):
                task = tasks.tasks.get_at(i)
                result = WorkerTaskResult()
                result.request_id = task.request_id
                result.task_id = task.task_id
                result.client_id = task.client_id

                try:
                    output_path = run_paths.outputs / f"{task.task_id}.jpg"
                    if output_path.exists():
                        output_bytes = output_path.read_bytes()
                        result.status = "OK"
                        result.output_blob = ModelRunner._to_bytes(output_bytes)
                    else:
                        result.status = "ERROR_NO_OUTPUT"
                        result.output_blob = ModelRunner._empty_bytes()
                except Exception as e:
                    print(f"Error reading output: {e}")
                    result.status = "ERROR"
                    result.output_blob = ModelRunner._empty_bytes()

                results_seq.set_at(i, result)

            worker_result.results = results_seq
            return worker_result

        except Exception as e:
            print(f"Batch processing error: {e}")
            # Fallback to single task processing
            for i in range(n):
                task = tasks.tasks.get_at(i)
                result = ModelRunner._run_single_task(task)
                results_seq.set_at(i, result)

            worker_result.results = results_seq
            return worker_result

        finally:
            if run_paths:
                ModelRunner._delete_recursive_quietly(run_paths.root)

    @staticmethod
    def _run_single_task(task: Task) -> WorkerTaskResult:
        """Single task fallback, clean up when done"""
        result = WorkerTaskResult()
        result.request_id = task.request_id
        result.task_id = task.task_id
        result.client_id = task.client_id

        run_paths = None
        try:
            if not task or not task.payload or task.payload.length() == 0:
                result.status = "ERROR_INVALID_INPUT"
                result.output_blob = ModelRunner._empty_bytes()
                return result

            run_paths = ModelRunner._make_single_run_paths(task.task_id, task.request_id)

            in_len = task.payload.length()
            in_bytes = bytearray(in_len)
            task.payload.to_array(in_bytes, in_len)

            input_path = run_paths.inputs / f"{task.task_id}.jpg"
            ModelRunner._atomic_write(input_path, bytes(in_bytes))

            ModelRunner._run_model_single(str(input_path), task.task_id, str(run_paths.outputs))

            output_path = run_paths.outputs / f"{task.task_id}.jpg"
            output_bytes = output_path.read_bytes()

            result.status = "OK"
            result.output_blob = ModelRunner._to_bytes(output_bytes)
            return result

        except Exception as e:
            print(f"Single task error: {e}")
            result.status = "ERROR"
            result.output_blob = ModelRunner._empty_bytes()
            return result

        finally:
            if run_paths:
                ModelRunner._delete_recursive_quietly(run_paths.root)

    # ========== Python model execution ==========

    @staticmethod
    def _run_model_batch(input_dir: str, output_dir: str) -> None:
        """Run batch model inference"""
        try:
            python_exe = ModelRunner._find_python_executable()
            script_path = ModelRunner._find_python_script_path()

            cmd = [
                python_exe,
                script_path,
                "--path", input_dir,
                "--out_dir", output_dir
            ]

            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            # Print output
            if process.stdout:
                for line in process.stdout.splitlines():
                    print(f"[PYTHON] {line}")

            if process.stderr:
                for line in process.stderr.splitlines():
                    print(f"[PYTHON ERROR] {line}")

            print(f"batch exitcode={process.returncode}")

        except subprocess.TimeoutExpired:
            print("Python process timed out")
            raise
        except Exception as e:
            print(f"Error running Python batch: {e}")
            raise

    @staticmethod
    def _run_model_single(input_path: str, task_id: str, output_dir: str) -> None:
        """Run single model inference"""
        try:
            python_exe = ModelRunner._find_python_executable()
            script_path = ModelRunner._find_python_script_path()

            cmd = [
                python_exe,
                script_path,
                "--path", input_path,
                "--task_id", task_id,
                "--out_dir", output_dir
            ]

            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60  # 1 minute timeout for single task
            )

            # Print output
            if process.stdout:
                for line in process.stdout.splitlines():
                    print(f"[PYTHON] {line}")

            if process.stderr:
                for line in process.stderr.splitlines():
                    print(f"[PYTHON ERROR] {line}")

            print(f"single exitcode={process.returncode}")

        except subprocess.TimeoutExpired:
            print("Python process timed out")
            raise
        except Exception as e:
            print(f"Error running Python single: {e}")
            raise

    # ========== Path/directory utilities ==========

    @staticmethod
    def _make_batch_run_paths(batch_id: str) -> RunPaths:
        """Create paths for batch run"""
        safe_batch = ModelRunner._safe_name(batch_id, "batch")
        run_id = f"{int(time.time() * 1000)}_{str(uuid.uuid4())[:8]}"

        root = Path("workdir") / "batches" / safe_batch / run_id
        inputs = root / "inputs"
        outputs = root / "outputs"

        inputs.mkdir(parents=True, exist_ok=True)
        outputs.mkdir(parents=True, exist_ok=True)

        return RunPaths(run_id, root, inputs, outputs)

    @staticmethod
    def _make_single_run_paths(task_id: str, request_id: str) -> RunPaths:
        """Create paths for single task run"""
        safe_task = ModelRunner._safe_name(task_id, "task")
        safe_req = ModelRunner._safe_name(request_id, "req")
        run_id = f"{int(time.time() * 1000)}_{str(uuid.uuid4())[:8]}"

        root = Path("workdir") / "singles" / f"{safe_task}_{safe_req}_{run_id}"
        inputs = root / "inputs"
        outputs = root / "outputs"

        inputs.mkdir(parents=True, exist_ok=True)
        outputs.mkdir(parents=True, exist_ok=True)

        return RunPaths(run_id, root, inputs, outputs)

    @staticmethod
    def _atomic_write(target_path: Path, data: bytes) -> None:
        """Atomic file write"""
        temp_path = target_path.with_suffix(target_path.suffix + ".part")

        try:
            temp_path.write_bytes(data)
            # Try atomic move first
            try:
                temp_path.replace(target_path)
            except OSError:
                # Fallback to regular move
                if target_path.exists():
                    target_path.unlink()
                temp_path.rename(target_path)
        except Exception:
            # Clean up temp file if something went wrong
            if temp_path.exists():
                temp_path.unlink()
            raise

    @staticmethod
    def _delete_recursive_quietly(root_path: Optional[Path]) -> None:
        """Delete directory recursively with retries"""
        if not root_path or not root_path.exists():
            return

        for attempt in range(3):
            try:
                shutil.rmtree(root_path, ignore_errors=True)
                return
            except Exception as e:
                if attempt < 2:  # Not the last attempt
                    time.sleep(0.08 * (attempt + 1))
                else:
                    print(f"Failed to delete {root_path}: {e}")

    # ========== Path finding / Bytes utilities ==========

    @staticmethod
    def _safe_name(name: str, default: str = "unknown") -> str:
        """Make a safe filename"""
        if not name:
            return default

        # Replace unsafe characters with underscores
        safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
        return safe if safe else default

    @staticmethod
    def _find_python_executable() -> str:
        """Find Python executable"""
        # Try common Python executables
        candidates = ["python", "python3"]

        if platform.system() == "Windows":
            candidates.extend([
                "py",
                r"C:\Users\HWH\AppData\Local\Programs\Python\Python39\python.exe"
            ])

        for candidate in candidates:
            try:
                result = subprocess.run(
                    [candidate, "--version"],
                    capture_output=True,
                    timeout=5
                )
                if result.returncode == 0:
                    return candidate
            except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
                continue

        # Default fallback
        if platform.system() == "Windows":
            return r"C:\Users\HWH\AppData\Local\Programs\Python\Python39\python.exe"
        else:
            return "python3"

    @staticmethod
    def _find_python_script_path() -> str:
        """Find Python script path"""
        # Try relative path first
        script_paths = [
            Path("src") / "yolo_service" / "pred.py",
            Path.cwd() / "src" / "yolo_service" / "pred.py",
            Path("yolo_service") / "pred.py",
            Path("pred.py")
        ]

        for script_path in script_paths:
            if script_path.exists():
                return str(script_path)

        # Default fallback
        return str(Path.cwd() / "src" / "yolo_service" / "pred.py")

    @staticmethod
    def _empty_bytes() -> bytearray:
        """Create empty Bytes object"""
        b = bytearray()
        b.from_array(bytearray(0), 0)
        return b

    @staticmethod
    def _to_bytes(data: bytes) -> bytearray:
        """Convert bytes to Bytes object"""
        b = bytearray()
        b.from_array(bytearray(data), len(data))
        return b


# Example usage and testing
if __name__ == "__main__":
    print("ModelRunner Python implementation")

    # Test path creation
    try:
        paths = ModelRunner._make_batch_run_paths("test_batch_123")
        print(f"Created batch paths: {paths}")

        # Test file operations
        test_data = b"Hello, World!"
        test_file = paths.inputs / "test.txt"
        ModelRunner._atomic_write(test_file, test_data)

        if test_file.exists():
            read_data = test_file.read_bytes()
            print(f"File write/read test: {'PASS' if read_data == test_data else 'FAIL'}")

        # Cleanup
        ModelRunner._delete_recursive_quietly(paths.root)
        print("Cleanup completed")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback

        traceback.print_exc()