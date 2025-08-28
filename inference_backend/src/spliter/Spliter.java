package spliter;

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

import data_structure.*; // WorkerResult / WorkerTaskResult / WorkerTaskResultSeq / ResultUpdate / ResultItem / *TypeSupport

import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * spliter.Spliter（服务端去重 + 流式增量发布）
 * - 订阅: WorkerResult
 * - 发布: ResultUpdate（按 request_id 分组，带 seq_no；客户端仅按 TTL 判定是否“收齐”）
 * - 去重键: (batch_id, request_id, task_id)
 */
public class Spliter {

    private static final int DOMAIN_ID = 100;
    private static final String FACTORY_PROFILE     = "non_rio";
    private static final String PARTICIPANT_PROFILE = "tcp_dp";

    private static final String WORKER_READER_QOS   = "non_zerocopy_reliable";
    private static final String UPDATE_WRITER_QOS   = "non_zerocopy_reliable";

    private static final String TOPIC_WORKER_RESULT = "inference/worker_result";
    private static final String TOPIC_RESULT_UPDATE = "inference/result_update";

    // ===== 参数（可根据需要调） =====
    /** 去重表保留时长（毫秒），超过删除，防内存增长 */
    private static final long DEDUP_TTL_MS = 60_000;
    /** 去重表清理周期 */
    private static final long CLEANUP_INTERVAL_MS = 10_000;

    /** 去重表：key = batch_id|request_id|task_id -> 最近一次看到的时间戳 */
    private static final ConcurrentMap<String, Long> SEEN = new ConcurrentHashMap<>();

    // ============ Listener ============
    private static class WorkerResultListener extends SimpleDataReaderListener<WorkerResult, WorkerResultSeq, WorkerResultDataReader> {
        private final ResultUpdateDataWriter updateWriter;
        private final ScheduledExecutorService cleaner;

        WorkerResultListener(ResultUpdateDataWriter writer) {
            this.updateWriter = writer;
            this.cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "spliter-cleaner");
                t.setDaemon(true);
                return t;
            });
            this.cleaner.scheduleAtFixedRate(this::cleanupDedup, CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        @Override
        public void on_process_sample(DataReader reader, WorkerResult wr, SampleInfo info) {
            try {
                if (wr == null || wr.results == null || wr.results.length() == 0) {
                    System.err.println("[Spliter] empty WorkerResult or results");
                    return;
                }

                // 1) 先把新结果做服务端去重，并按 request_id 分组
                //    map: request_id -> (client_id, List<ResultItem>)
                Map<String, Group> groups = new HashMap<>();

                int n = wr.results.length();
                for (int i = 0; i < n; i++) {
                    WorkerTaskResult r = (WorkerTaskResult) wr.results.get_at(i);
                    if (r == null) continue;

                    final String reqId = r.request_id;
                    final String clientId = r.client_id; // 由链路透传，作为回路由键
                    final String key = dedupKey(wr.batch_id, reqId, r.task_id);

                    // 服务端去重
                    Long last = SEEN.put(key, System.currentTimeMillis());
                    if (last != null) {
                        // 已处理过，丢弃
                        continue;
                    }

                    Group g = groups.computeIfAbsent(reqId, k -> new Group(clientId));
                    g.items.add(toItem(r));
                }

                // 2) 对每个 request_id 生成一条 ResultUpdate（seq_no 单调递增）
                for (Map.Entry<String, Group> e : groups.entrySet()) {
                    String requestId = e.getKey();
                    Group g = e.getValue();
                    if (g.items.isEmpty()) continue;

                    ResultUpdate upd = new ResultUpdate();
                    upd.request_id = requestId;
                    upd.client_id = g.clientId;

                    // build ResultItemSeq
                    ResultItemSeq seq = new ResultItemSeq();
                    int m = g.items.size();
                    seq.ensure_length(m, m);
                    for (int i = 0; i < m; i++) {
                        seq.set_at(i, g.items.get(i));
                    }
                    upd.items = seq;

                    ReturnCode_t rc = updateWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    if (rc != ReturnCode_t.RETCODE_OK) {
                        System.err.println("[Spliter] write ResultUpdate failed: " + rc);
                    } else {
                        System.out.println("[Spliter] Published ResultUpdate req=" + requestId +
                                 " items=" + m);
                    }
                }

            } catch (Throwable t) {
                System.err.println("[Spliter] on_process_sample error: " + t.getMessage());
                t.printStackTrace();
            }
        }

        private static class Group {
            final String clientId;
            final List<ResultItem> items = new ArrayList<>();
            Group(String clientId) { this.clientId = clientId; }
        }

        private static String dedupKey(String batchId, String reqId, String taskId) {
            // 简单组合键，避免分配过多对象
            StringBuilder sb = new StringBuilder(
                    (batchId==null?0:batchId.length()) +
                            (reqId==null?0:reqId.length()) +
                            (taskId==null?0:taskId.length()) + 8);
            sb.append(batchId).append('|').append(reqId).append('|').append(taskId);
            return sb.toString();
        }

        private static ResultItem toItem(WorkerTaskResult r) {
            ResultItem it = new ResultItem();
            it.task_id = r.task_id;
            it.status  = r.status;
            // 直接拷贝 Bytes（注意：这里引用同一 Bytes 实例即可，不必再复制）
            it.output_blob = r.output_blob;
            return it;
        }

        private void cleanupDedup() {
            long now = System.currentTimeMillis();
            long ttl = Math.max(10_000L, DEDUP_TTL_MS);
            int scanned = 0;
            int removed = 0;
            // 避免全表扫描过久，限制每次扫描上限
            int SCAN_LIMIT = 50_000;

            for (Iterator<Map.Entry<String, Long>> it = SEEN.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, Long> e = it.next();
                scanned++;
                if (now - e.getValue() > ttl) {
                    it.remove();
                    removed++;
                }
                if (scanned >= SCAN_LIMIT) break;
            }
            if (removed > 0) {
                System.out.println("[Spliter] dedup cleanup removed=" + removed + " scanned=" + scanned);
            }
        }

        public void shutdown() {
            if (cleaner != null && !cleaner.isShutdown()) {
                cleaner.shutdownNow();
            }
        }

        @Override
        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
            // 可选：不需要
        }
    }

    // ====================== Main ======================
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

            // 2) 注册类型
            WorkerResultTypeSupport wrTS = (WorkerResultTypeSupport) WorkerResultTypeSupport.get_instance();
            ResultUpdateTypeSupport upTS = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();

            if (wrTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || upTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
                System.err.println("register type failed");
                return;
            }

            // 3) 创建 Topic
            Topic wrTopic = dp.create_topic(
                    TOPIC_WORKER_RESULT,
                    wrTS.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null, StatusKind.STATUS_MASK_NONE
            );

            Topic upTopic = dp.create_topic(
                    TOPIC_RESULT_UPDATE,
                    upTS.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null, StatusKind.STATUS_MASK_NONE
            );

            if (wrTopic == null || upTopic == null) {
                System.err.println("create topic failed");
                return;
            }

            // 4) Pub / Sub
            Publisher pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

            // 5) Writer / Reader
            ResultUpdateDataWriter upWriter = (ResultUpdateDataWriter)
                    pub.create_datawriter(upTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (upWriter == null) {
                System.err.println("ResultUpdate writer creation failed");
                return;
            }

            WorkerResultDataReader wrReader = (WorkerResultDataReader)
                    sub.create_datareader(wrTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (wrReader == null) {
                System.err.println("WorkerResult reader creation failed");
                return;
            }

            // 6) Listener
            listener = new WorkerResultListener(upWriter);
            wrReader.set_listener(listener, StatusKind.DATA_AVAILABLE_STATUS);

            System.out.println("==================================================");
            System.out.println("Spliter started (server-side dedup + streaming updates)");
            System.out.println("Dedup TTL: " + DEDUP_TTL_MS + " ms; Cleanup: " + CLEANUP_INTERVAL_MS + " ms");
            System.out.println("Topics: " + TOPIC_WORKER_RESULT + " -> " + TOPIC_RESULT_UPDATE);
            System.out.println("==================================================");
            System.out.println("Press ENTER to exit...");
            System.in.read();

        } finally {
            if (listener != null) listener.shutdown();
            DDSIF.Finalize();
            System.out.println("Spliter stopped.");
        }
    }
}
