package state_monitor;

import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.Topic;
import data_structure.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;

/**
 * StateMonitor类 - 监控Worker的健康状态
 * 使用DDS的Liveliness QoS机制来监控Worker是否在指定时间内发送心跳信号
 */
public class StateMonitor {

    private static final int DOMAIN_ID = 100;
    private static final String TOPIC_WORKER_HEARTBEAT = "monitor/worker_heartbeat";

    /* ===================== 配置参数 ===================== */
    public static class Config {
        public final String monitorId;
        public final int heartbeatTimeoutSeconds; // 心跳超时时间（秒）
        public final int checkIntervalSeconds;     // 检查间隔（秒）

        public Config(String monitorId, int heartbeatTimeoutSeconds, int checkIntervalSeconds) {
            this.monitorId = monitorId;
            this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
            this.checkIntervalSeconds = checkIntervalSeconds;
        }
    }

    /* ===================== 健康状态回调接口 ===================== */
    public interface HealthStatusCallback {
        void onWorkerHealthy(String workerId);
        void onWorkerFailed(String workerId);
        void onWorkerRecovered(String workerId);
    }

    /* ===================== 成员变量 ===================== */
    private final Config config;
    private final HealthStatusCallback callback;

    // DDS组件
    private DomainParticipant domainParticipant;
    private Subscriber subscriber;
    private WorkerResultDataReader heartbeatReader;

    // 监控状态
    private final ConcurrentHashMap<String, Long> workerLastSeen = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> workerHealthStatus = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    // 监控线程
    private ScheduledExecutorService monitorExecutor;
    private Thread heartbeatThread;

    public StateMonitor(Config config, HealthStatusCallback callback) {
        this.config = config;
        this.callback = callback;
    }

    /* ===================== 生命周期管理 ===================== */

    public boolean start() {
        if (running.getAndSet(true)) {
            System.out.println("StateMonitor already running");
            return false;
        }

        try {
            // 初始化DDS组件
            if (!initializeDDS()) {
                System.err.println("Failed to initialize DDS components");
                stop();
                return false;
            }

            // 启动监控线程
            startMonitoringThreads();

            System.out.println("StateMonitor started successfully");
            System.out.println("Monitor ID: " + config.monitorId);
            System.out.println("Heartbeat timeout: " + config.heartbeatTimeoutSeconds + " seconds");
            System.out.println("Check interval: " + config.checkIntervalSeconds + " seconds");

            return true;

        } catch (Exception e) {
            System.err.println("Failed to start StateMonitor: " + e.getMessage());
            e.printStackTrace();
            stop();
            return false;
        }
    }

    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        System.out.println("Stopping StateMonitor...");

        // 停止监控线程
        if (monitorExecutor != null) {
            monitorExecutor.shutdown();
            try {
                if (!monitorExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    monitorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                monitorExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }

        // 清理DDS资源
        cleanupDDS();

        System.out.println("StateMonitor stopped");
    }

    /* ===================== DDS初始化和清理 ===================== */

    private boolean initializeDDS() {
        try {
            // 创建DomainParticipant
            DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
            if (dpf == null) {
                System.err.println("Failed to get DomainParticipantFactory");
                return false;
            }

            domainParticipant = dpf.create_participant(
                    DOMAIN_ID,
                    DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );
            if (domainParticipant == null) {
                System.err.println("Failed to create DomainParticipant");
                return false;
            }

            // 注册数据类型
            if (WorkerResultTypeSupport.get_instance().register_type(domainParticipant, null) != ReturnCode_t.RETCODE_OK) {
                System.err.println("Failed to register WorkerResult type");
                return false;
            }

            // 创建Topic
            Topic heartbeatTopic = domainParticipant.create_topic(
                    TOPIC_WORKER_HEARTBEAT,
                    WorkerResultTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );
            if (heartbeatTopic == null) {
                System.err.println("Failed to create heartbeat topic");
                return false;
            }

            // 创建Subscriber
            subscriber = domainParticipant.create_subscriber(
                    DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );
            if (subscriber == null) {
                System.err.println("Failed to create Subscriber");
                return false;
            }

            // 创建DataReader with Liveliness QoS
            DataReaderQos readerQos = new DataReaderQos();
            subscriber.get_default_datareader_qos(readerQos);

            // 配置Liveliness QoS
            readerQos.liveliness.kind = LivelinessQosPolicyKind.MANUAL_BY_TOPIC_LIVELINESS_QOS;
            readerQos.liveliness.lease_duration.sec = config.heartbeatTimeoutSeconds;
            readerQos.liveliness.lease_duration.nanosec = 0;

            DataReader reader = subscriber.create_datareader(
                    heartbeatTopic,
                    readerQos,
                    new HeartbeatReaderListener(),
                    StatusKind.DATA_AVAILABLE_STATUS | StatusKind.LIVELINESS_CHANGED_STATUS
            );

            heartbeatReader = (WorkerResultDataReader) reader;
            if (heartbeatReader == null) {
                System.err.println("Failed to create DataReader");
                return false;
            }

            return true;

        } catch (Exception e) {
            System.err.println("Exception during DDS initialization: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void cleanupDDS() {
        try {
            if (domainParticipant != null) {
                domainParticipant.delete_contained_entities();
                DomainParticipantFactory.get_instance().delete_participant(domainParticipant);
                domainParticipant = null;
            }
        } catch (Exception e) {
            System.err.println("Error during DDS cleanup: " + e.getMessage());
        }
    }

    /* ===================== 监控线程 ===================== */

    private void startMonitoringThreads() {
        // 定期检查Worker健康状态的线程
        monitorExecutor = Executors.newScheduledThreadPool(1);
        monitorExecutor.scheduleAtFixedRate(
                this::checkWorkerHealth,
                config.checkIntervalSeconds,
                config.checkIntervalSeconds,
                TimeUnit.SECONDS
        );

        // 处理心跳数据的线程
        heartbeatThread = new Thread(this::processHeartbeats, "heartbeat-processor");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private void checkWorkerHealth() {
        if (!running.get()) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        long timeoutMillis = config.heartbeatTimeoutSeconds * 1000L;

        for (Map.Entry<String, Long> entry : workerLastSeen.entrySet()) {
            String workerId = entry.getKey();
            long lastSeenTime = entry.getValue();

            boolean isHealthy = (currentTime - lastSeenTime) <= timeoutMillis;
            Boolean previousStatus = workerHealthStatus.get(workerId);

            if (previousStatus == null || previousStatus != isHealthy) {
                workerHealthStatus.put(workerId, isHealthy);

                if (isHealthy) {
                    if (previousStatus != null && !previousStatus) {
                        // Worker恢复健康
                        System.out.println("[StateMonitor] Worker recovered: " + workerId);
                        callback.onWorkerRecovered(workerId);
                    } else {
                        // Worker首次检测到健康
                        System.out.println("[StateMonitor] Worker healthy: " + workerId);
                        callback.onWorkerHealthy(workerId);
                    }
                } else {
                    // Worker失效
                    System.out.println("[StateMonitor] Worker failed: " + workerId +
                            " (last seen " + (currentTime - lastSeenTime) + "ms ago)");
                    callback.onWorkerFailed(workerId);
                }
            }
        }
    }

    private void processHeartbeats() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // 这里主要是为了保持线程活跃，实际的心跳处理在listener中
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /* ===================== DDS监听器 ===================== */

    private class HeartbeatReaderListener implements DataReaderListener {

        @Override
        public void on_data_available(DataReader reader) {
            WorkerResultDataReader heartbeatReader = (WorkerResultDataReader) reader;
            WorkerResultSeq dataSeq = new WorkerResultSeq();
            SampleInfoSeq infoSeq = new SampleInfoSeq();

            try {
                ReturnCode_t result = heartbeatReader.read(
                        dataSeq, infoSeq, -1,
                        SampleStateKind.ANY_SAMPLE_STATE,
                        ViewStateKind.ANY_VIEW_STATE,
                        InstanceStateKind.ANY_INSTANCE_STATE
                );

                if (result == ReturnCode_t.RETCODE_OK) {
                    for (int i = 0; i < infoSeq.length(); i++) {
                        if (infoSeq.get_at(i).valid_data) {
                            WorkerResult heartbeat = (WorkerResult) dataSeq.get_at(i);
                            if (heartbeat != null && heartbeat.worker_id != null) {
                                // 更新Worker最后心跳时间
                                workerLastSeen.put(heartbeat.worker_id, System.currentTimeMillis());

                                System.out.println("[StateMonitor] Heartbeat received from worker: " +
                                        heartbeat.worker_id);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[StateMonitor] Error processing heartbeat data: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    heartbeatReader.return_loan(dataSeq, infoSeq);
                } catch (Exception e) {
                    System.err.println("[StateMonitor] Error returning loan: " + e.getMessage());
                }
            }
        }

        @Override
        public void on_liveliness_changed(DataReader reader, LivelinessChangedStatus status) {
            System.out.println("[StateMonitor] Liveliness changed - " +
                    "alive_count: " + status.alive_count +
                    ", not_alive_count: " + status.not_alive_count);

            // 当有Worker的liveliness发生变化时，可以在这里处理
            if (status.not_alive_count > 0) {
                System.out.println("[StateMonitor] Some workers may have failed (liveliness lost)");
            }
        }

        @Override
        public void on_requested_deadline_missed(DataReader reader, RequestedDeadlineMissedStatus status) {}

        @Override
        public void on_requested_incompatible_qos(DataReader reader, RequestedIncompatibleQosStatus status) {}

        @Override
        public void on_sample_lost(DataReader reader, SampleLostStatus status) {}

        @Override
        public void on_sample_rejected(DataReader reader, SampleRejectedStatus status) {}

        @Override
        public void on_subscription_matched(DataReader reader, SubscriptionMatchedStatus status) {
            System.out.println("[StateMonitor] Subscription matched - current_count: " + status.current_count);
        }

        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {}
    }

    /* ===================== 公共接口 ===================== */

    /**
     * 获取指定Worker的健康状态
     */
    public Boolean getWorkerHealthStatus(String workerId) {
        return workerHealthStatus.get(workerId);
    }

    /**
     * 获取所有Worker的健康状态
     */
    public Map<String, Boolean> getAllWorkerHealthStatus() {
        return new ConcurrentHashMap<>(workerHealthStatus);
    }

    /**
     * 获取指定Worker的最后心跳时间
     */
    public Long getWorkerLastSeenTime(String workerId) {
        return workerLastSeen.get(workerId);
    }

    /* ===================== 测试主函数 ===================== */

    public static void main(String[] args) {
        System.loadLibrary("ZRDDS_JAVA");

        StateMonitor.Config config = new StateMonitor.Config(
                "monitor-1",        // 监控器ID
                10,                 // 心跳超时时间: 10秒
                5                   // 检查间隔: 5秒
        );

        StateMonitor.HealthStatusCallback callback = new StateMonitor.HealthStatusCallback() {
            @Override
            public void onWorkerHealthy(String workerId) {
                System.out.println(">>> CALLBACK: Worker " + workerId + " is HEALTHY");
            }

            @Override
            public void onWorkerFailed(String workerId) {
                System.out.println(">>> CALLBACK: Worker " + workerId + " FAILED!");
            }

            @Override
            public void onWorkerRecovered(String workerId) {
                System.out.println(">>> CALLBACK: Worker " + workerId + " RECOVERED!");
            }
        };

        StateMonitor monitor = new StateMonitor(config, callback);

        if (monitor.start()) {
            System.out.println("==================================================");
            System.out.println("StateMonitor started successfully!");
            System.out.println("Monitoring topic: " + TOPIC_WORKER_HEARTBEAT);
            System.out.println("Press ENTER to stop...");
            System.out.println("==================================================");

            try {
                System.in.read();
            } catch (Exception e) {
                e.printStackTrace();
            }

            monitor.stop();
        } else {
            System.err.println("Failed to start StateMonitor");
        }
    }
}