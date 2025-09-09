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
from threading import Event, Lock
import signal

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


class LRUDict(OrderedDict):
    """Thread-safe LRU dictionary with maximum size"""

    def __init__(self, max_size=4096):
        super().__init__()
        self.max_size = max_size
        self._lock = Lock()

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
    max_inflight_batches: int = 2
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
                Task.__name__,
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
            self.claim_writer = self.pub.create_datawriter(self.claim_topic, DDS_All.DATAREADER_QOS_DEFAULT, None, mask)
            self.result_writer = self.pub.create_datawriter(self.result_topic, DDS_All.DATAREADER_QOS_DEFAULT, None, mask)

            heartbeat_qos = DataWriterQos()
            self.pub.get_default_datawriter_qos(heartbeat_qos)

            self.heartbeat_writer = self.pub.create_datawriter(
                self.heartbeat_topic,
                DDS_All.DATAREADER_QOS_DEFAULT,
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

            if not (rtn_open is DDS_All.DDS_ReturnCode_t.OK and rtn_task is DDS_All.DDS_ReturnCode_t.OK ):
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

    def result_emitter(self,worker_result: WorkerResult) -> None:
        rc = self.result_writer.write(worker_result)
        if rc != DDS_All.DDS_ReturnCode_t.OK:
            print(f"[WorkerMain] result write rc={rc}")

    def on_open_batch(self, open_batch: OpenBatch) -> None:
        """Handle OpenBatch message"""
        if not open_batch or open_batch.model_id != self.config.model_id:
            return

        if self.current_depth() >= self.config.max_inflight_batches:
            return

        if open_batch.batch_id in self.claimed_lru:
            return

        self.claimed_lru[open_batch.batch_id] = True

        claim = Claim()
        claim.batch_id = open_batch.batch_id
        claim.worker_id = self.config.worker_id
        claim.queue_length = self.current_depth()

        try:
            self.claim_emitter(claim)
        except Exception as e:
            print(f"Error emitting claim: {e}")

    def on_task_list(self, task_list: TaskList) -> None:
        """Handle TaskList message"""
        if not task_list:
            return

        if (task_list.assigned_worker_id != self.config.worker_id or
                task_list.model_id != self.config.model_id):
            return

        with self.seen_lock:
            if task_list.batch_id in self.seen_task_list:
                return
            self.seen_task_list.add(task_list.batch_id)

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

    def _consume_loop(self) -> None:
        """Main consumer loop"""
        while self.running.is_set():
            try:
                task_list = self.batch_queue.get(timeout=0.1)

                with self.inflight_lock:
                    self.inflight_count += 1

                try:
                    worker_result = self.model_runner.run_batched_task(task_list)
                except Exception as e:
                    worker_result = self._synthesize_error_result(task_list, e)

                worker_result.batch_id = task_list.batch_id
                worker_result.model_id = task_list.model_id
                worker_result.worker_id = self.config.worker_id

                try:
                    self.result_emitter(worker_result)
                except Exception as e:
                    print(f"Error emitting result: {e}")

                finally:
                    with self.inflight_lock:
                        self.inflight_count -= 1

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error in consume loop: {e}")
                with self.inflight_lock:
                    self.inflight_count -= 1

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
            result.output_blob = bytearray()
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
            if rc == DDS_ReturnCode_t.OK:
                print(f"[Worker] Heartbeat sent: {self.config.worker_id}")
            else:
                print(f"[Worker] Failed to send heartbeat, rc={rc}")

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

    def on_process_sample(self, reader: DataReader, sample: OpenBatch, info) -> None:
        if sample:
            try:
                self.worker.on_open_batch(sample)
            except Exception as e:
                print(f"Error processing OpenBatch: {e}")

    def on_data_arrived(self, reader: DataReader, sample, info) -> None:
        # Required abstract method, can be empty
        pass

class TaskListListener(DataReaderListener):
    """Listener for TaskList messages"""

    def __init__(self, worker: Worker):
        super().__init__()
        self.worker = worker

    def on_process_sample(self, reader: DataReader, sample: TaskList, info) -> None:
        if sample:
            try:
                self.worker.on_task_list(sample)
            except Exception as e:
                print(f"Error processing TaskList: {e}")

    def on_data_arrived(self, reader: DataReader, sample, info) -> None:
        # Required abstract method, can be empty
        pass


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

        model_runner=ModelRunner()

        worker = Worker(config,model_runner)

        res=worker._initialize_dds()
        if not res:
            print("worker dds initialize failed")
        print("worker dds环境初始化成功")

        # Start worker
        worker.start()

        print("=" * 50)
        print(f"Worker started. worker_id={worker_id} model_id={model_id}")
        print(f"Sub: {Worker.TOPIC_OPEN_BATCH}, {Worker.TOPIC_TASK_LIST}")
        print(f"Pub: {Worker.TOPIC_CLAIM}, {Worker.TOPIC_WORKER_RESULT}")
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