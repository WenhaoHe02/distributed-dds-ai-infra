#!/usr/bin/env python3
"""
Worker (Scheme B, Claim updated: {batch_id, worker_id, queue_length})
Python version of the Java Worker class
"""
import os
import sys
import time
import threading
import queue
import random
import string
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable, Dict, Set
from dataclasses import dataclass
from threading import Event, Lock,RLock
import signal
import json
from pathlib import Path

import DDS_All
from DDS_All import *
from DDS_All import DomainParticipantFactory
from DDS_All import DataReader
from DDS_All import DataWriterQos
from DDS_All import Subscriber

# Data structure imports
from DDS_All import (
    OpenBatch, Claim, TaskList, WorkerResult, WorkerTaskResult,
    WorkerResultDataWriter, WorkerTaskResultSeq,Task
)

from model_runner import ModelRunner

def load_worker_config(model_id: str, base_dir: str = "configs") -> dict:
    """
    从 worker/configs 读取 {model_id}_worker.conf.json；也支持 env 覆盖：
    WORKER_CONFIG_FILE=/abs/path/to/xxx.json
    """
    override = os.environ.get("WORKER_CONFIG_FILE")
    if override:
        cfg_path = Path(override)
    else:
        cfg_path = Path(base_dir) / f"{model_id}_worker.conf.json"

    if not cfg_path.exists():
        raise FileNotFoundError(f"Worker config not found: {cfg_path}")

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    if "model_id" not in cfg:
        cfg["model_id"] = model_id
    if cfg["model_id"] != model_id:
        print(f"[WARN] model_id in config({cfg['model_id']}) != worker model_id({model_id}), using worker's model_id.")
        cfg["model_id"] = model_id

    if "model_config" not in cfg or "parameter" not in cfg["model_config"]:
        raise KeyError("config.model_config.parameter is required")

    return cfg


class LRUDict(OrderedDict):
    """Thread-safe LRU dictionary with maximum size"""

    def __init__(self, max_size=4096):
        super().__init__()
        self.max_size = max_size
        self._lock = RLock()

    def __setitem__(self, key, value):
        with self._lock:
            if key in self:
                self.move_to_end(key)
            super().__setitem__(key, value)
            if len(self) > self.max_size:
                self.popitem(last=False)

    def __getitem__(self, key):
        with self._lock:
            value = super().__getitem__(key)
            self.move_to_end(key)
            return value

    def __contains__(self, key):
        with self._lock:
            return super().__contains__(key)


@dataclass
class WorkerConfig:
    """Configuration for Worker"""
    worker_id: str
    model_id: str
    max_inflight_batches: int = 64
    queue_capacity: int = 64
    enable_heartbeat: bool = True
    heartbeat_interval_seconds: int = 5


class Worker:
    """Python Worker implementation"""

    # Constants
    DOMAIN_ID = 100
    TOPIC_OPEN_BATCH = "inference/open_batch"
    TOPIC_CLAIM = "inference/claim"
    TOPIC_TASK_LIST = "inference/task_list"
    TOPIC_WORKER_RESULT = "inference/worker_result"
    TOPIC_WORKER_HEARTBEAT = "monitor/worker_heartbeat"

    def __init__(self,
                 config: WorkerConfig,
                 model_runner: ModelRunner):

        self.config = config
        self.model_runner = model_runner

        # State
        self.batch_queue = queue.Queue(maxsize=config.queue_capacity)
        self.inflight_count = 0
        self.inflight_lock = Lock()

        self.claimed_lru = LRUDict(max_size=4096)
        self.seen_task_list: Set[str] = set()
        self.seen_lock = Lock()

        self.running = Event()
        self.consumer_thread: Optional[threading.Thread] = None

        # DDS组件
        self.dp: Optional[DomainParticipant] = None
        self.sub: Optional[Subscriber] = None
        self.pub: Optional[Publisher] = None

        self.open_topic: Optional[Topic] = None
        self.claim_topic: Optional[Topic] = None
        self.task_topic: Optional[Topic] = None
        self.result_topic: Optional[Topic] = None
        self.heartbeat_topic: Optional[Topic] = None

        self.claim_writer: Optional[ClaimDataWriter] = None
        self.result_writer: Optional[WorkerResultDataWriter] = None
        self.heartbeat_writer: Optional[WorkerResultDataWriter] = None
        self.openbatch_reader: Optional[OpenBatchDataReader] = None
        self.tasklist_reader: Optional[TaskListDataReader] = None

        self.openbatch_listener: Optional[OpenBatchListener] = None
        self.tasklist_listener: Optional[TaskListListener] = None

        # Heartbeat
        self.heartbeat_enabled = Event()
        self.heartbeat_executor: Optional[ThreadPoolExecutor] = None

    def _initialize_dds(self) -> bool:
        """初始化DDS组件"""
        try:
            # 1.从工厂创建DomainParticipant
            dpf = DomainParticipantFactory.get_instance()
            if not dpf:
                print("DomainParticipantFactory.get_instance() failed")
                return False

            self.dp = dpf.create_participant(
                self.DOMAIN_ID,
                DDS_All.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0
            )
            if not self.dp:
                print("create_participant failed")
                return False
            print("成功创建DomainParticipant")

            # 2.注册类型
            DDS_All.register_all_types(self.dp)

            # 3.创建 Publisher 和 Subscriber
            self.pub = self.dp.create_publisher(-1, None, 0)
            self.sub = self.dp.create_subscriber(-1, None, 0)

            if not self.pub or not self.sub:
                print("create_publisher/subscriber failed")
                return False
            print("成功创建Publisher和Subscriber")

            # 4.创建Topic
            self.open_topic = self.dp.create_topic(
                Worker.TOPIC_OPEN_BATCH,
                OpenBatch.__name__,
                DDS_All.TOPIC_QOS_DEFAULT,
                None,
                0
            )
            self.claim_topic = self.dp.create_topic(
                Worker.TOPIC_CLAIM,
                Claim.__name__,
                DDS_All.TOPIC_QOS_DEFAULT,
                None,
                0
            )
            self.task_topic = self.dp.create_topic(
                Worker.TOPIC_TASK_LIST,
                TaskList.__name__,
                DDS_All.TOPIC_QOS_DEFAULT,
                None,
                0
            )
            self.result_topic = self.dp.create_topic(
                Worker.TOPIC_WORKER_RESULT,
                WorkerResult.__name__,
                DDS_All.TOPIC_QOS_DEFAULT,
                None,
                0
            )
            self.heartbeat_topic = self.dp.create_topic(
                Worker.TOPIC_WORKER_HEARTBEAT,
                WorkerResult.__name__,
                DDS_All.TOPIC_QOS_DEFAULT,
                None,
                0
            )
            if not all([self.open_topic, self.claim_topic, self.task_topic, self.result_topic, self.heartbeat_topic]):
                print("create_topic failed")
                return False
            print("成功创建Topic")

            # 5.创建DateReadeer 和 DateWriter
            mask = StatusKind.DATA_AVAILABLE_STATUS | StatusKind.SUBSCRIPTION_MATCHED_STATUS
            self.claim_writer = self.pub.create_datawriter(self.claim_topic, DDS_All.DATAWRITER_QOS_DEFAULT, None, mask)
            self.result_writer = self.pub.create_datawriter(self.result_topic, DDS_All.DATAWRITER_QOS_DEFAULT, None, mask)

            heartbeat_qos = DataWriterQos()
            self.pub.get_default_datawriter_qos(heartbeat_qos)

            self.heartbeat_writer = self.pub.create_datawriter(
                self.heartbeat_topic,
                heartbeat_qos,
                None,
                mask
            )

            if not all([self.claim_writer, self.result_writer, self.heartbeat_writer]):
                print("create_datawriter failed")
                return False
            print("成功创建Writers")

            # Create data readers with listeners
            self.openbatch_reader = self.sub.create_datareader(
                self.open_topic,
                DDS_All.DATAREADER_QOS_DEFAULT,
                None,
                0
            )

            self.tasklist_reader = self.sub.create_datareader(
                self.task_topic,
                DDS_All.DATAREADER_QOS_DEFAULT,
                None,
                0
            )

            if not self.openbatch_reader or not self.tasklist_reader:
                print("create_datareader failed")
                return False

            print("成功创建Readers")

            self.openbatch_listener=OpenBatchListener(self)
            self.tasklist_listener=TaskListListener(self)

            # Set listeners
            rtn_open=self.openbatch_reader.set_listener(
                self.openbatch_listener,
                StatusKind.DATA_AVAILABLE_STATUS
            )

            rtn_task=self.tasklist_reader.set_listener(
                self.tasklist_listener,
                StatusKind.DATA_AVAILABLE_STATUS
            )

            if not (rtn_open == DDS_All.DDS_ReturnCode_t.OK and rtn_task == DDS_All.DDS_ReturnCode_t.OK ):
                print("associate listener failed")
                return False
            print("成功绑定listener")

            return True

        except Exception as e:
            print(f"Exception during DDS initialization: {e}")
            import traceback
            traceback.print_exc()
            return False

    def claim_emitter(self,claim: Claim) -> None:
        rc = self.claim_writer.write(claim)
        if rc != DDS_All.DDS_ReturnCode_t.OK:
            print(f"[WorkerMain] claim write rc={rc}")
        else:
            print("[Worker]成功发送claim batch_id: ",claim.batch_id," worker_id: ",claim.worker_id," queue_length: ",claim.queue_length)

    def result_emitter(self,worker_result: WorkerResult) -> None:
        rc = self.result_writer.write(worker_result)
        if rc != DDS_All.DDS_ReturnCode_t.OK:
            print(f"[WorkerMain] result write rc={rc}")
        else:
            print("[Worker]成功发送WokerResult")

    def on_open_batch(self, open_batch: OpenBatch) -> None:
        """Handle OpenBatch message"""
        if not open_batch or open_batch.model_id != self.config.model_id:
            print("[Worker]模型不符")
            return

        if self.current_depth() >= self.config.max_inflight_batches:
            print("[Worker]队列不足")
            return

        if open_batch.batch_id in self.claimed_lru:
            print("[Worker]batch已存在")
            return

        self.claimed_lru[open_batch.batch_id] = True

        claim = Claim()

        claim.batch_id = open_batch.batch_id
        claim.worker_id = self.config.worker_id
        claim.queue_length = self.current_depth()

        print("[Worker]get batch batch_id: ",claim.batch_id," worker_id: ",claim.worker_id," queue_length: ",claim.queue_length)

        try:
            self.claim_emitter(claim)
        except Exception as e:
            print(f"Error emitting claim: {e}")

    def on_task_list(self, task_list: TaskList) -> None:
        """Handle TaskList message"""
        if  not task_list:
            return
        if (task_list.assigned_worker_id != self.config.worker_id or
                task_list.model_id != self.config.model_id):
            return

        with self.seen_lock:
            if task_list.batch_id in self.seen_task_list:
                return
            self.seen_task_list.add(task_list.batch_id)

        print("[Worker]get tasklist worker_id: ", task_list.assigned_worker_id, " batch_id: ",
              task_list.batch_id, " model_id: ", task_list.model_id)
        #"tasks: ",task_list.tasks
        try:
            self.batch_queue.put_nowait(task_list)
        except queue.Full:
            try:
                # Remove oldest and add new
                self.batch_queue.get_nowait()
                self.batch_queue.put_nowait(task_list)
            except queue.Empty:
                pass

    def start(self) -> None:
        """Start the worker"""
        if self.running.is_set():
            return

        self.running.set()
        self.consumer_thread = threading.Thread(
            target=self._consume_loop,
            name=f"worker-consumer-{self.config.worker_id}",
            daemon=True
        )
        self.consumer_thread.start()

        self._start_heartbeat()

    def stop(self) -> None:
        """Stop the worker"""
        self.running.clear()
        self._stop_heartbeat()

        if self.consumer_thread:
            self.consumer_thread.join(timeout=5.0)

    def _consume_loop(self):
        """
        取批 → 运行模型 → 回写结果 的工作线程主循环。
        关键保证：
          - 只有在成功 get 到任务后才递增 inflight_count；
          - 自减在内层 finally，任何路径都能配对成功；
          - 不对 queue.Empty 做“白减”；
          - 对外上报/内部统计都不让 inflight 出现负数；
          - 支持优雅停机（stop_event / _stop_event）。
        """
        import time, queue, logging, traceback
        logger = getattr(self, "log", logging.getLogger("Worker"))

        stop_flag = getattr(self, "stop_event", None) or getattr(self, "_stop_event", None)
        get_timeout = getattr(self, "queue_get_timeout", 0.1) or 0.1  # 秒

        while True:
            # 支持优雅停机
            if stop_flag and stop_flag.is_set():
                break

            try:
                # 只要没有拿到任务，绝不增/减 inflight
                task_list = self.batch_queue.get(timeout=get_timeout)
            except queue.Empty:
                continue
            except Exception as e:
                # 取任务本身异常（极少），记录后继续
                logger.exception("[Worker] batch_queue.get() failed: %s", e)
                continue

            # === 从这里开始才算“在途” ===
            start_ts = time.time()
            with self.inflight_lock:
                self.inflight_count += 1

            try:
                try:
                    # 运行模型
                    result = self.model_runner.run_batched_task(task_list)
                except Exception as run_err:
                    # 兜底：生成错误结果，尽量复用你的现有方法
                    logger.exception(
                        "[Worker] run_batched_task failed for batch_id=%s: %s",
                        getattr(task_list, "batch_id", "?"), run_err
                    )
                    if hasattr(self, "_synthesize_error_result"):
                        result = self._synthesize_error_result(task_list, run_err)
                    else:
                        # 最小兜底：构造一个带错误信息的结果对象/字典
                        try:
                            # 如果有 DDS_All 类型可用，就尽量用它
                            from DDS_All import WorkerResult
                            result = WorkerResult()
                            result.batch_id = getattr(task_list, "batch_id", "")
                            result.worker_id = getattr(getattr(self, "config", object()), "worker_id", "")
                            # 不同 IDL 字段名可能不同，尽量宽松处理
                            setattr(result, "status", "ERROR")
                            setattr(result, "error_msg", f"runner exception: {run_err}")
                        except Exception:
                            # 退回到 dict（写入时你的 writer 可能不接受 dict，但至少不至于空对象）
                            result = {
                                "batch_id": getattr(task_list, "batch_id", ""),
                                "worker_id": getattr(getattr(self, "config", object()), "worker_id", ""),
                                "status": "ERROR",
                                "error_msg": f"{run_err}",
                            }

                # 写回结果
                try:
                    self.result_writer.write(result)
                except Exception as write_err:
                    logger.exception(
                        "[Worker] result_writer.write failed for batch_id=%s: %s",
                        getattr(task_list, "batch_id", "?"), write_err
                    )

            finally:
                # 无论成功/失败，都配对自减 + task_done
                with self.inflight_lock:
                    self.inflight_count -= 1
                    # 保底不让内部计数为负（外部上报再取 max(0, ...)）
                    if self.inflight_count < 0:
                        self.inflight_count = 0

                # 队列配对完成
                try:
                    self.batch_queue.task_done()
                except Exception:
                    pass

                # 统计与可观测性
                dur_ms = (time.time() - start_ts) * 1000.0
                try:
                    qsize = self.batch_queue.qsize()
                except Exception:
                    qsize = -1
                logger.info(
                    "[Worker] finished batch_id=%s in %.1f ms | inflight=%d | qsize=%s",
                    getattr(task_list, "batch_id", "?"),
                    dur_ms,
                    getattr(self, "inflight_count", -1),
                    qsize,
                )

        # 退出前的收尾日志
        logger.info("[Worker] consume loop exited")


    def _synthesize_error_result(self, task_list: TaskList, error: Exception) -> WorkerResult:
        """Create error result for failed batch"""
        worker_result = WorkerResult()
        worker_result.batch_id = task_list.batch_id
        worker_result.model_id = task_list.model_id
        worker_result.worker_id = self.config.worker_id

        results = WorkerTaskResultSeq()
        n = task_list.tasks.length() if task_list.tasks else 0
        results.ensure_length(n, n)

        for i in range(n):
            task = task_list.tasks.get_at(i)
            result = WorkerTaskResult()
            result.request_id = task.request_id
            result.task_id = task.task_id
            result.client_id = task.client_id
            result.status = "ERROR_RUNNER"
            result.output_blob = b''
            results.set_at(i, result)

        worker_result.results = results
        return worker_result

    def current_depth(self) -> int:
        """Get current processing depth"""
        with self.inflight_lock:
            return self.inflight_count + self.batch_queue.qsize()

    def set_heartbeat_writer(self, heartbeat_writer: WorkerResultDataWriter) -> None:
        """Set heartbeat writer (called from main)"""
        self.heartbeat_writer = heartbeat_writer

    def _start_heartbeat(self) -> None:
        """Start heartbeat sending"""
        if not self.config.enable_heartbeat or not self.heartbeat_writer:
            return

        self.heartbeat_enabled.set()
        self.heartbeat_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="heartbeat")

        def heartbeat_loop():
            while self.heartbeat_enabled.is_set():
                try:
                    self._send_heartbeat()
                    time.sleep(self.config.heartbeat_interval_seconds)
                except Exception as e:
                    print(f"Error in heartbeat loop: {e}")
                    if not self.heartbeat_enabled.is_set():
                        break

        self.heartbeat_executor.submit(heartbeat_loop)
        print(f"[Worker] Heartbeat started, interval: {self.config.heartbeat_interval_seconds} seconds")

    def _stop_heartbeat(self) -> None:
        """Stop heartbeat sending"""
        self.heartbeat_enabled.clear()

        if self.heartbeat_executor:
            self.heartbeat_executor.shutdown(wait=True)
            self.heartbeat_executor = None

        print("[Worker] Heartbeat stopped")

    def _send_heartbeat(self) -> None:
        """Send heartbeat signal"""
        if not self.heartbeat_enabled.is_set() or not self.heartbeat_writer:
            return

        try:
            # Create heartbeat message (reusing WorkerResult structure)
            heartbeat = WorkerResult()
            heartbeat.worker_id = self.config.worker_id
            heartbeat.model_id = self.config.model_id
            heartbeat.batch_id = f"HEARTBEAT_{int(time.time() * 1000)}"

            # Send heartbeat
            rc = self.heartbeat_writer.write(heartbeat)
            # if rc == DDS_ReturnCode_t.OK:
            #     print(f"[Worker] Heartbeat sent: {self.config.worker_id}")
            # else:
            #     print(f"[Worker] Failed to send heartbeat, rc={rc}")

        except Exception as e:
            print(f"[Worker] Error sending heartbeat: {e}")


def sys_or_env(sys_key: str, env_key: str, default_val: str) -> str:
    """Get value from system property or environment variable"""
    value = os.environ.get(sys_key.upper().replace('.', '_'))
    if not value:
        value = os.environ.get(env_key)
    return value or default_val


def generate_random_id() -> str:
    """Generate random worker ID"""
    random_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"worker-{random_part}-cpu"


class OpenBatchListener(DataReaderListener):
    """Listener for OpenBatch messages"""

    def __init__(self, worker: Worker):
        super().__init__()
        self.worker = worker

    def on_data_available(self, reader:DDS_All.OpenBatchDataReader):
        data_seq = OpenBatchSeq()
        info_seq = SampleInfoSeq()

        try:
            result = reader.take(
                data_seq, info_seq, -1,
                DDS_All.ANY_SAMPLE_STATE,
                DDS_All.ANY_VIEW_STATE,
                DDS_All.ANY_INSTANCE_STATE
            )

            if result == DDS_All.DDS_ReturnCode_t.OK:
                for i in range(info_seq.length()):
                    info = info_seq.get_at(i)
                    if info and info.valid_data:
                        openbatch = data_seq.get_at(i)
                        if openbatch:
                            self.worker.on_open_batch(openbatch)

        except Exception as e:
            print(f"[Worker] Error processing: {e}")
        finally:
            try:
                reader.return_loan(data_seq, info_seq)
            except Exception:
                pass

class TaskListListener(DataReaderListener):
    """Listener for TaskList messages"""

    def __init__(self, worker: Worker):
        super().__init__()
        self.worker = worker

    def on_data_available(self, reader:DDS_All.TaskListDataReader):
        data_seq = TaskListSeq()
        info_seq = SampleInfoSeq()

        try:
            result = reader.take(
                data_seq, info_seq, -1,
                DDS_All.ANY_SAMPLE_STATE,
                DDS_All.ANY_VIEW_STATE,
                DDS_All.ANY_INSTANCE_STATE
            )

            if result == DDS_All.DDS_ReturnCode_t.OK:
                for i in range(info_seq.length()):
                    info = info_seq.get_at(i)
                    if info and info.valid_data:
                        tasklist = data_seq.get_at(i)
                        if tasklist:
                            self.worker.on_task_list(tasklist)

        except Exception as e:
            print(f"[Worker] Error processing: {e}")
        finally:
            try:
                reader.return_loan(data_seq, info_seq)
            except Exception:
                pass

    # def on_process_sample(self, reader: DataReader, sample: TaskList, info) -> None:
    #     if sample:
    #         try:
    #             self.worker.on_task_list(sample)
    #         except Exception as e:
    #             print(f"Error processing TaskList: {e}")
    #
    # def on_data_arrived(self, reader: DataReader, sample, info) -> None:
    #     pass


def main():
    """Main function"""
    worker_id = sys_or_env("worker.id", "WORKER_ID", "worker1_cpu")
    model_id = sys_or_env("worker.model", "WORKER_MODEL", "model_0")

    worker = None

    def cleanup():
        """Cleanup function"""
        nonlocal worker
        try:
            if worker:
                worker.stop()
        except:
            pass
        print("Worker stopped.")

    def signal_handler(signum, frame):
        """Signal handler for graceful shutdown"""
        print(f"\nReceived signal {signum}, shutting down...")
        cleanup()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        config = WorkerConfig(worker_id, model_id)
        config.enable_heartbeat = True
        config.heartbeat_interval_seconds = 5
        base_model_config = load_worker_config(model_id, base_dir="configs")
        model_runner = ModelRunner(base_model_config)
        worker = Worker(WorkerConfig(worker_id, model_id), model_runner)

        res=worker._initialize_dds()
        if not res:
            print("worker dds initialize failed")
        print("worker dds环境初始化成功")

        # Start worker
        worker.start()

        print("=" * 50)
        print(f"Worker started. worker_id={worker_id} model_id={model_id}")
        print(f"Sub: {Worker.TOPIC_OzzaPEN_BATCH}, {Worker.TOPIC_TASK_LIST}")
        print(f"Pub: {Worker.TOPIC_CLAIM}, {Worker.TaOPIC_WORKER_RESULT}")
        print(f"Heartbeat enabled: {config.enable_heartbeat} (interval: {config.heartbeat_interval_seconds}s)")
        print("Press ENTER to exit...")
        print("=" * 50)

        input()  # Wait for user input

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup()


if __name__ == "__main__":
    main()