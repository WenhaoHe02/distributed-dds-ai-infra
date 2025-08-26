package aggregator;

import com.zrdds.infrastructure.StatusKind;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.SampleInfo;
import com.zrdds.topic.Topic;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;

import data_structure.*; // WorkerResult / WorkerResultSeq / WorkerResultDataReader / AggregatedResult* / *TypeSupport

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.Queue;

/**
 * aggregator.Aggregator:
 * - Subscribe: workerResult
 * - Publish  : AggregatedResult
 * - 汇总来自不同Worker的同一request_id的结果并发送
 */
public class Aggregator {

    private static final int DOMAIN_ID = 100;
    private static final String FACTORY_PROFILE     = "non_rio";
    private static final String PARTICIPANT_PROFILE = "tcp_dp";

    private static final String WORKER_READER_QOS   = "non_zerocopy_reliable";
    private static final String AGG_WRITER_QOS      = "non_zerocopy_reliable";

    private static final String TOPIC_WORKER_RESULT = "workerResult";
    private static final String TOPIC_AGG_RESULT    = "AggregatedResult";

    // 配置参数
    private static final int EXPECTED_WORKER_COUNT = 3;  // 预期的Worker数量
    private static final long AGGREGATION_TIMEOUT_MS = 5000;  // 聚合超时时间（毫秒）
    private static final long CLEANUP_INTERVAL_MS = 10000;    // 清理过期数据间隔

    // 用于存储按request_id分组的结果
    private static class RequestAggregation {
        final String requestId;
        final String clientId;
        final Queue<WorkerResult> workerResults;
        final long creationTime;

        RequestAggregation(String requestId, String clientId) {
            this.requestId = requestId;
            this.clientId = clientId;
            this.workerResults = new ConcurrentLinkedQueue<>();
            this.creationTime = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - creationTime > AGGREGATION_TIMEOUT_MS;
        }
    }

    // Listener：收到 WorkerResult 就按request_id汇总，满足条件时发布 AggregatedResult
    private static class WorkerResultListener extends SimpleDataReaderListener<WorkerResult, WorkerResultSeq, WorkerResultDataReader> {
        private final AggregatedResultDataWriter aggWriter;
        private final Map<String, RequestAggregation> pendingRequests;
        private final ScheduledExecutorService scheduler;

        WorkerResultListener(AggregatedResultDataWriter writer) {
            this.aggWriter = writer;
            this.pendingRequests = new ConcurrentHashMap<>();
            this.scheduler = Executors.newSingleThreadScheduledExecutor();

            // 定期清理过期的请求
            this.scheduler.scheduleAtFixedRate(this::cleanupExpiredRequests,
                    CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        @Override
        public void on_process_sample(DataReader reader, WorkerResult sample, SampleInfo info) {
            try {
                System.out.println("=============================================");

                if (sample == null || sample.results == null || sample.results.length() == 0) {
                    System.err.println("[Aggregator] Received empty WorkerResult");
                    return;
                }

                System.out.println("[Aggregator] Received WorkerResult with " + sample.results.length() + " results");

                // 临时map: request_id -> 结果列表
                Map<String, SingleResultSeq> groupedResults = new HashMap<>();

                // 遍历 WorkerResult 中的每个 SingleResult
                for (int i = 0; i < sample.results.length(); i++) {
                    SingleResult result = sample.results.get_at(i);
                    if (result == null || result.task == null) continue;

                    String requestId = result.task.request_id;
                    String clientId  = result.task.client_id;

                    // 找到该 requestId 对应的组
                    SingleResultSeq seq = groupedResults.computeIfAbsent(requestId, k -> new SingleResultSeq());
                    seq.append(result);

                    // 打印结果
                    SingleResultTypeSupport.get_instance().print_sample(result);
                }

                // 遍历分好组的结果，每组生成一个 AggregatedResult 并发布
                for (Map.Entry<String, SingleResultSeq> entry : groupedResults.entrySet()) {
                    String requestId = entry.getKey();
                    SingleResultSeq results = entry.getValue();

                    if (results == null || results.length() == 0) continue;

                    // 假设所有结果的 client_id 一致，取第一个的
                    String clientId = results.get_at(0).task.client_id;

                    AggregatedResult aggResult = new AggregatedResult();
                    aggResult.request_id = requestId;
                    aggResult.client_id  = clientId;
                    aggResult.results    = results;
                    aggResult.status     = "AGGREGATED";
                    aggResult.error_message = "";

                    ReturnCode_t rc = aggWriter.write(aggResult, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    if (rc != ReturnCode_t.RETCODE_OK) {
                        System.err.println("[Aggregator] Write AggregatedResult failed: " + rc);
                    } else {
                        System.out.println("[Aggregator] Published AggregatedResult for request_id=" +
                                requestId + ", results=" + results.length());
                    }
                }

            } catch (Throwable t) {
                System.err.println("[Aggregator] Error processing sample: " + t.getMessage());
                t.printStackTrace();
            }
        }



        /*
        @Override
        public void on_process_sample(DataReader reader, WorkerResult sample, SampleInfo info) {
            try {

                System.out.println("=============================================");

                if (sample == null || sample.results == null || sample.results.length() == 0) {
                    System.err.println("[Aggregator] Received empty WorkerResult");
                    return;
                }

                // 获取第一个结果的request_id和client_id
                SingleResult firstResult = sample.results.get_at(0);
                String requestId = firstResult.task.request_id;
                String clientId = firstResult.task.client_id;

                System.out.println("[Aggregator] Received WorkerResult for request: " + requestId +
                        ", client: " + clientId + ", result_num: " + sample.result_num +
                        ", actual results: " + sample.results.length());

                // 获取或创建该request_id的聚合器
                RequestAggregation aggregation = pendingRequests.computeIfAbsent(requestId,
                        k -> new RequestAggregation(requestId, clientId));

                // 添加当前WorkerResult到聚合器
                aggregation.workerResults.offer(sample);

                // 检查是否满足发布条件
                if (shouldPublishAggregatedResult(aggregation)) {
                    publishAggregatedResult(aggregation);
                    pendingRequests.remove(requestId);
                }

            } catch (Throwable t) {
                System.err.println("[Aggregator] Error processing sample: " + t.getMessage());
                t.printStackTrace();
            }
        }
        */


        private void publishAggregatedResult(RequestAggregation aggregation) {
            try {
                AggregatedResult aggResult = new AggregatedResult();
                aggResult.request_id = aggregation.requestId;
                aggResult.client_id  = aggregation.clientId;
                aggResult.status     = "AGGREGATED";
                aggResult.error_message = "";

                aggResult.results = new SingleResultSeq();
                int totalResults = 0;

                for (WorkerResult wrapper : aggregation.workerResults) {
                    if (wrapper.results != null && wrapper.results.length() > 0) {
                        SingleResult sr = wrapper.results.get_at(0);
                        aggResult.results.append(sr);
                        totalResults++;
                    }
                }

                // 如果超时才发布
                if (aggregation.isExpired()) {
                    aggResult.status = "PARTIAL_AGGREGATED";
                    aggResult.error_message = "Aggregated with timeout, received " +
                            totalResults + " SingleResult(s), expected from " +
                            EXPECTED_WORKER_COUNT + " workers.";
                }

                ReturnCode_t rc = aggWriter.write(aggResult, InstanceHandle_t.HANDLE_NIL_NATIVE);
                if (rc != ReturnCode_t.RETCODE_OK) {
                    System.err.println("[Aggregator] Write AggregatedResult failed: " + rc);
                } else {
                    System.out.println("[Aggregator] AggregatedResult published for request: " +
                            aggResult.request_id + ", total SingleResult count: " + totalResults);
                }
            } catch (Throwable t) {
                System.err.println("[Aggregator] Error publishing aggregated result: " + t.getMessage());
                t.printStackTrace();
            }
        }


        /*
        private void publishAggregatedResult(RequestAggregation aggregation) {
            try {
                // 构造 AggregatedResult
                AggregatedResult aggResult = new AggregatedResult();
                aggResult.request_id = aggregation.requestId;
                aggResult.client_id = aggregation.clientId;
                aggResult.status = "AGGREGATED";
                aggResult.error_message = "";

                // 汇总所有WorkerResult的结果
                aggResult.results = new SingleResultSeq();
                int totalResults = 0;

                for (WorkerResult workerResult : aggregation.workerResults) {
                    if (workerResult.results != null) {
                        for (int i = 0; i < workerResult.results.length(); i++) {
                            SingleResult result = workerResult.results.get_at(i);
                            if (result != null) {
                                aggResult.results.append(result);
                                totalResults++;
                            }
                        }
                    }
                }

                // 如果因为超时发布，更新状态
                if (aggregation.isExpired()) {
                    aggResult.status = "PARTIAL_AGGREGATED";
                    aggResult.error_message = "Aggregated with timeout, received " +
                            aggregation.workerResults.size() + " out of " +
                            EXPECTED_WORKER_COUNT + " expected worker results";
                }

                ReturnCode_t rc = aggWriter.write(aggResult, InstanceHandle_t.HANDLE_NIL_NATIVE);
                if (rc != ReturnCode_t.RETCODE_OK) {
                    System.err.println("[Aggregator] Write AggregatedResult failed: " + rc);
                } else {
                    System.out.println("[Aggregator] AggregatedResult published for request: " +
                            aggResult.request_id + ", worker count: " +
                            aggregation.workerResults.size() + ", total results: " + totalResults);
                }
            } catch (Throwable t) {
                System.err.println("[Aggregator] Error publishing aggregated result: " + t.getMessage());
                t.printStackTrace();
            }
        }
        */

        private void cleanupExpiredRequests() {
            try {
                long currentTime = System.currentTimeMillis();
                pendingRequests.entrySet().removeIf(entry -> {
                    RequestAggregation aggregation = entry.getValue();
                    if (aggregation.isExpired()) {
                        System.out.println("[Aggregator] Processing expired request: " + entry.getKey());
                        publishAggregatedResult(aggregation);
                        return true;  // 移除该条目
                    }
                    return false;
                });
            } catch (Throwable t) {
                System.err.println("[Aggregator] Error during cleanup: " + t.getMessage());
                t.printStackTrace();
            }
        }

        public void shutdown() {
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
            // 可选实现
        }
    }

    public static void main(String[] args) throws Exception {
        WorkerResultListener listener = null;

        try {
            // 1) 初始化 DDS
            DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
            if (dpf == null) {
                System.err.println("DDSIF.init failed");
                return;
            }
            DomainParticipant dp = dpf.create_participant(
                    DOMAIN_ID,
                    DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                    null, StatusKind.STATUS_MASK_NONE);
            if (dp == null) { System.err.println("create dp failed"); return; }


            // 2) 创建数据类型
            WorkerResultTypeSupport workerTS = (WorkerResultTypeSupport) WorkerResultTypeSupport.get_instance();
            AggregatedResultTypeSupport aggTS = (AggregatedResultTypeSupport) AggregatedResultTypeSupport.get_instance();

            if (workerTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || aggTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
                System.err.println("register type failed");
                return;
            }

            // 3) 创建 Topic
            Topic workerTopic = dp.create_topic(
                    TOPIC_WORKER_RESULT,
                    workerTS.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );

            Topic aggTopic = dp.create_topic(
                    TOPIC_AGG_RESULT,
                    aggTS.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );

            if (workerTopic == null || aggTopic == null) {
                System.err.println("create topic failed");
                return;
            }

            // 4) 创建 Publisher / Subscriber
            Publisher pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

            // 5) 创建 DataWriter
            AggregatedResultDataWriter aggWriter  = (AggregatedResultDataWriter)
                    pub.create_datawriter(aggTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE); // 可选
            if (aggWriter == null) {
                System.err.println("aggWriter creation failed");
                return;
            }

            // 6) 创建 Listener 和 DataReader
            listener = new WorkerResultListener(aggWriter);

            WorkerResultDataReader workerReader = (WorkerResultDataReader)
                    sub.create_datareader(workerTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (workerReader == null) {
                System.err.println("workerReader creation failed");
                return;
            }

            // 7) listeners
            workerReader.set_listener(listener,        StatusKind.STATUS_MASK_ALL);

            System.out.println("==================================================");
            System.out.println("Aggregator started successfully!");
            System.out.println("Expected worker count: " + EXPECTED_WORKER_COUNT);
            System.out.println("Aggregation timeout: " + AGGREGATION_TIMEOUT_MS + "ms");
            System.out.println("==================================================");
            System.out.println("Press ENTER to exit...");

            System.in.read();

        } finally {
            // 清理资源
            if (listener != null) {
                listener.shutdown();
            }
            DDSIF.Finalize();
            System.out.println("Aggregator stopped.");
        }
    }
}