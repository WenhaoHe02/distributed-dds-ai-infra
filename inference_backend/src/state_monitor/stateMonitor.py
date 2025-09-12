"""
StateMonitor - 监控Worker的健康状态
使用DDS的Liveliness QoS机制来监控Worker是否在指定时间内发送心跳信号
Python版本的Java StateMonitor类
"""
import os
import sys
import time
import threading
import signal
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from typing import Optional, Dict, Protocol
from dataclasses import dataclass

import DDS_All
from DDS_All import *
from DDS_All import DomainParticipantFactory, DataReader
from DDS_All import Subscriber
from DDS_All import WorkerResult, WorkerResultSeq, SampleInfoSeq
import logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"

# 2. 配置 logging
logging.basicConfig(
    level=logging.INFO,        # 设置日志级别：DEBUG < INFO < WARNING < ERROR < CRITICAL
    format=log_format          # 设置输出格式
)

@dataclass
class StateMonitorConfig:
    """StateMonitor配置"""
    monitor_id: str
    heartbeat_timeout_seconds: int
    check_interval_seconds: int


class HealthStatusCallback(Protocol):
    """健康状态回调接口"""

    def on_worker_healthy(self, worker_id: str) -> None:
        """Worker健康时调用"""
        ...

    def on_worker_failed(self, worker_id: str) -> None:
        """Worker失效时调用"""
        ...

    def on_worker_recovered(self, worker_id: str) -> None:
        """Worker恢复时调用"""
        ...

class StateMonitor:
    """监控Worker健康状态的主类"""

    # Constants
    DOMAIN_ID = 100
    TOPIC_WORKER_HEARTBEAT = "monitor/worker_heartbeat"

    def __init__(self, config: StateMonitorConfig, callback: HealthStatusCallback):
        self.config = config
        self.callback = callback

        # DDS组件
        self.domain_participant: Optional[DomainParticipant] = None
        self.subscriber: Optional[Subscriber] = None
        self.heartbeat_reader: Optional[DataReader] = None
        self.heartbeatListener=None

        # 监控状态
        self.worker_last_seen: Dict[str, float] = {}
        self.worker_health_status: Dict[str, bool] = {}
        self.worker_lock = Lock()

        # 监控线程控制
        self.running = Event()
        self.monitor_executor: Optional[ThreadPoolExecutor] = None
        self.heartbeat_thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        """启动监控器"""
        if self.running.is_set():
            logging.info("StateMonitor already running")
            return False
        else:
            self.running.set()

        try:
            # 初始化DDS组件
            if not self._initialize_dds():
                logging.error("Failed to initialize DDS components")
                self.stop()
                return False

            # 启动监控线程
            self._start_monitoring_threads()

            logging.info("StateMonitor started successfully")
            logging.info(f"Monitor ID: %s",self.config.monitor_id)
            logging.info(f"Heartbeat timeout: %s seconds",self.config.heartbeat_timeout_seconds)
            logging.info(f"Check interval: %s seconds",self.config.check_interval_seconds)

            return True

        except Exception as e:
            logging.error(f"Failed to start StateMonitor: %s",e)
            self.stop()
            return False

    def stop(self) -> None:
        """停止监控器"""
        if not self.running.is_set():
            return

        logging.info("Stopping StateMonitor...")
        self.running.clear()

        # 停止监控线程
        if self.monitor_executor:
            self.monitor_executor.shutdown(wait=True)
            self.monitor_executor = None

        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5.0)
            self.heartbeat_thread = None

        # 清理DDS资源
        self._cleanup_dds()

        logging.info("StateMonitor stopped")

    def _initialize_dds(self) -> bool:
        """初始化DDS组件"""
        try:
            # 创建DomainParticipant
            dpf = DomainParticipantFactory.get_instance()
            if not dpf:
                logging.error("Failed to get DomainParticipantFactory")
                return False

            self.domain_participant = dpf.create_participant(
                self.DOMAIN_ID,
                DDS_All.DOMAINPARTICIPANT_QOS_DEFAULT,
                None,
                0
            )
            if not self.domain_participant:
                logging.error("Failed to create DomainParticipant")
                return False
            logging.info("成功创建DomainParticipant")

            # 注册数据类型
            DDS_All.register_all_types(self.domain_participant)

            # 创建Topic
            heartbeat_topic = self.domain_participant.create_topic(
                self.TOPIC_WORKER_HEARTBEAT,
                WorkerResult.__name__,
                DDS_All.TOPIC_QOS_DEFAULT,
                None,
                0
            )
            if not heartbeat_topic:
                logging.error("Failed to create heartbeat topic")
                return False
            logging.info("成功创建Topic")

            # 创建Subscriber
            self.subscriber = self.domain_participant.create_subscriber(
                DDS_All.SUBSCRIBER_QOS_DEFAULT,
                None,
                0
            )
            if not self.subscriber:
                logging.error("Failed to create Subscriber")
                return False
            logging.info("成功创建Subscriber")

            # 创建DataReader with Liveliness QoS
            reader_qos = DataReaderQos()
            self.subscriber.get_default_datareader_qos(reader_qos)

            self.heartbeatListener=HeartbeatReaderListener(self)
            # 创建DataReader
            self.heartbeat_reader = self.subscriber.create_datareader(
                heartbeat_topic,
                DDS_All.DATAREADER_QOS_DEFAULT,
                self.heartbeatListener,
                1024
            )

            if not self.heartbeat_reader:
                logging.error("Failed to create DataReader")
                return False
            logging.info("成功创建Reader")

            return True

        except Exception as e:
            logging.error(f"Exception during DDS initialization: %s",e)
            return False

    def _cleanup_dds(self) -> None:
        """清理DDS资源"""
        try:
            if self.domain_participant:
                self.domain_participant.delete_contained_entities()
                DomainParticipantFactory.get_instance().delete_participant(self.domain_participant)
                self.domain_participant = None
        except Exception as e:
            logging.error(f"Error during DDS cleanup: %s",e)

    def _start_monitoring_threads(self) -> None:
        """启动监控线程"""
        # 定期检查Worker健康状态的线程
        self.monitor_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="monitor")

        def monitor_loop():
            while self.running.is_set():
                try:
                    self._check_worker_health()
                    time.sleep(self.config.check_interval_seconds)
                except Exception as e:
                    logging.error(f"Error in monitor loop: %s",e)
                    if not self.running.is_set():
                        break

        self.monitor_executor.submit(monitor_loop)

        # 处理心跳数据的线程（主要保持活跃，实际处理在listener中）
        self.heartbeat_thread = threading.Thread(
            target=self._process_heartbeats,
            name="heartbeat-processor",
            daemon=True
        )
        self.heartbeat_thread.start()

    def _check_worker_health(self) -> None:
        """检查Worker健康状态"""
        if not self.running.is_set():
            return
        current_time = time.time()
        timeout_seconds = self.config.heartbeat_timeout_seconds

        with self.worker_lock:
            for worker_id, last_seen_time in list(self.worker_last_seen.items()):
                is_healthy = (current_time - last_seen_time) <= timeout_seconds
                previous_status = self.worker_health_status.get(worker_id)

                if previous_status is None or previous_status != is_healthy:
                    self.worker_health_status[worker_id] = is_healthy

                    if is_healthy:
                        if previous_status is None:
                            # Worker首次检测到健康
                            logging.info(f"[StateMonitor] Worker healthy: %s",worker_id)
                            self.callback.on_worker_healthy(worker_id)
                        else:
                            # Worker恢复健康
                            logging.info(f"[StateMonitor] Worker recovered: %s",worker_id)
                            self.callback.on_worker_recovered(worker_id)
                    else:
                        # Worker失效
                        elapsed_ms = int((current_time - last_seen_time) * 1000)
                        logging.info(f"[StateMonitor] Worker failed: %s "
                              f"(last seen %s ms ago)",worker_id,elapsed_ms)
                        self.callback.on_worker_failed(worker_id)

    def _process_heartbeats(self) -> None:
        """处理心跳数据的线程（主要保持活跃）"""
        while self.running.is_set():
            try:
                time.sleep(1.0)
            except Exception as e:
                if self.running.is_set():
                    logging.error(f"Error in heartbeat processing thread: %s",e)
                break

    def _update_worker_heartbeat(self, worker_id: str) -> None:
        """更新Worker的心跳时间"""
        with self.worker_lock:
            self.worker_last_seen[worker_id] = time.time()

    # Public API
    def get_worker_health_status(self, worker_id: str) -> Optional[bool]:
        """获取指定Worker的健康状态"""
        with self.worker_lock:
            return self.worker_health_status.get(worker_id)

    def get_all_worker_health_status(self) -> Dict[str, bool]:
        """获取所有Worker的健康状态"""
        with self.worker_lock:
            return self.worker_health_status.copy()

    def get_worker_last_seen_time(self, worker_id: str) -> Optional[float]:
        """获取指定Worker的最后心跳时间"""
        with self.worker_lock:
            return self.worker_last_seen.get(worker_id)

class HeartbeatReaderListener(DDS_All.DataReaderListener):
    """心跳数据监听器"""

    def __init__(self,statemonitor:StateMonitor):
        super().__init__()
        self.state_monitor=statemonitor

    def on_data_available(self, reader:DDS_All.WorkerResultDataReader):
        """处理心跳数据"""
        data_seq = WorkerResultSeq()
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
                        heartbeat = data_seq.get_at(i)
                        if heartbeat and heartbeat.worker_id:
                            # 更新Worker最后心跳时间
                            self.state_monitor._update_worker_heartbeat(heartbeat.worker_id)
                            logging.info(f"[StateMonitor] Heartbeat received from worker: %s",heartbeat.worker_id)

        except Exception as e:
            logging.error(f"[StateMonitor] Error processing heartbeat data: %s",e)
        finally:
            try:
                reader.return_loan(data_seq, info_seq)
            except Exception as e:
                logging.error(f"[StateMonitor] Error returning loan: %s",e)

    def on_liveliness_changed(self, reader: DataReader, status) -> None:
        """Liveliness状态变化时调用"""
        logging.info(f"[StateMonitor] Liveliness changed - "
              f"alive_count: %s, "
              f"not_alive_count: %s",status.alive_count,status.not_alive_count)

        if status.not_alive_count > 0:
            logging.info("[StateMonitor] Some workers may have failed (liveliness lost)")

    def on_requested_deadline_missed(self, reader: DataReader, status) -> None:
        pass

    def on_requested_incompatible_qos(self, reader: DataReader, status) -> None:
        pass

    def on_sample_lost(self, reader: DataReader, status) -> None:
        pass

    def on_sample_rejected(self, reader: DataReader, status) -> None:
        pass

    def on_subscription_matched(self, reader: DataReader, status) -> None:
        logging.info(f"[StateMonitor] Subscription matched - current_count: %s",status.current_count)

    def on_data_arrived(self, reader: DataReader, sample, info) -> None:
        # Required abstract method, can be empty
        pass


class TestHealthStatusCallback:
    """测试用的健康状态回调实现"""

    def on_worker_healthy(self, worker_id: str) -> None:
        logging.info(f">>> CALLBACK: Worker %s is HEALTHY",worker_id)

    def on_worker_failed(self, worker_id: str) -> None:
        logging.info(f">>> CALLBACK: Worker %s FAILED!",worker_id)

    def on_worker_recovered(self, worker_id: str) -> None:
        logging.info(f">>> CALLBACK: Worker %s RECOVERED!",worker_id)

def main():
    """测试主函数"""
    try:
        # 创建配置
        config = StateMonitorConfig(
            monitor_id="monitor-1",
            heartbeat_timeout_seconds=10,  # 心跳超时时间: 10秒
            check_interval_seconds=5  # 检查间隔: 5秒
        )

        # 创建回调
        callback = TestHealthStatusCallback()

        # 创建监控器
        monitor = StateMonitor(config, callback)

        # 信号处理
        def signal_handler(signum, frame):
            logging.info(f"\nReceived signal %s, shutting down...",signum)
            monitor.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 启动监控器
        if monitor.start():
            logging.info("=" * 50)
            logging.info("StateMonitor started successfully!")
            logging.info(f"Monitoring topic: %s",StateMonitor.TOPIC_WORKER_HEARTBEAT)
            logging.info("=" * 50)
            logging.info("正在监控数据...")
            while True:
                time.sleep(10)

    except Exception as e:
        logging.error(f"Error in main: %s",e)
        sys.exit(1)



if __name__ == "__main__":
    main()