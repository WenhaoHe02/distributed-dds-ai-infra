package spliter;

import com.zrdds.infrastructure.*;
import com.zrdds.publication.DataWriterQos;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.DataReaderQos;
import com.zrdds.subscription.SimpleDataReaderListener;
import com.zrdds.topic.Topic;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;

import data_structure.*; // WorkerResult / WorkerTaskResult / WorkerTaskResultSeq / ResultUpdate / ResultItem / *TypeSupport
import data_structure.Bytes;

import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * spliter.Spliter（服务端去重 + 微批聚合 + 空闲超时后的迟到丢弃）
 * - 订阅: WorkerResult
 * - 发布: ResultUpdate（按 request_id 分组微批，客户端按自身策略消费；本端仅空闲超时关闭请求并丢弃迟到）
 * - 去重键: (batch_id, request_id, task_id)
 */
public class Spliter {

    private static final int DOMAIN_ID = 100;

    private static final String TOPIC_WORKER_RESULT = "inference/worker_result";
    private static final String TOPIC_RESULT_UPDATE = "inference/result_update";

    /* ===================== 微批/超时参数（可按需调整） ===================== */
    /** 微批：每次最多聚合多少条；达到即 flush */
    private static final int  MAX_ITEMS_PER_UPDATE  = 32;

    /** 微批：字节上限（近似值，0 表示禁用字节阈值） */
    private static final int  MAX_BYTES_PER_UPDATE  = 0;

    /** 微批：自上次 flush 起等待的最大时长（毫秒），到达即 flush */
    private static final long MAX_WAIT_MS           = 6L;

    /** 空闲超时：自最近一次活动起超过该时长（毫秒），关闭请求并丢弃迟到 */
    private static final long IDLE_CLOSE_MS         = 30000L;

    /** 去重表：保留时长（毫秒） */
    private static final long DEDUP_TTL_MS          = 60_000L;

    /** 后台清理/定时 flush 的周期（毫秒） */
    private static final long CLEANUP_INTERVAL_MS   = 60_000L;
    private static final long FLUSH_TICK_MS         = 2L;

    /** 去重表：key = batch_id|request_id|task_id -> 首次看到的时间戳 */
    private static final ConcurrentMap<String, Long> SEEN = new ConcurrentHashMap<>();

    /** 每个 request 的微批状态 */
    private static final ConcurrentHashMap<String, ReqState> REQ = new ConcurrentHashMap<>();

    private static final class ReqState {
        final String requestId;
        String clientId;
        final ArrayList<ResultItem> buffer = new ArrayList<>(MAX_ITEMS_PER_UPDATE);
        long lastActivityTs;  // 最近一次收到该 request 的结果时间
        long lastFlushTs;     // 最近一次 flush 时间
        boolean closed = false;
        int approxBytes = 0;  // 近似字节数（可选）

        ReqState(String requestId, String clientId, long now){
            this.requestId = requestId;
            this.clientId  = clientId;
            this.lastActivityTs = now;
            this.lastFlushTs    = now;
        }
    }

    /* ===================== Listener ===================== */
    private static class WorkerResultListener extends SimpleDataReaderListener<WorkerResult, WorkerResultSeq, WorkerResultDataReader> {
        private final ResultUpdateDataWriter updateWriter;
        private final ScheduledExecutorService sched;

        WorkerResultListener(ResultUpdateDataWriter writer) {
            this.updateWriter = writer;
            this.sched = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "spliter-sched");
                t.setDaemon(true);
                return t;
            });

            // 定时清理去重表
            this.sched.scheduleAtFixedRate(this::cleanupDedup, CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
            // 定时：检查应当 flush 的请求 + 空闲关闭
            this.sched.scheduleAtFixedRate(this::flushDueRequestsSafe, FLUSH_TICK_MS, FLUSH_TICK_MS, TimeUnit.MILLISECONDS);
            // 定时：清理已关闭的请求状态
            this.sched.scheduleAtFixedRate(this::cleanupReqStates, CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        @Override
        public void on_process_sample(DataReader reader, WorkerResult wr, SampleInfo info) {
            try {
                if (wr == null || wr.results == null || wr.results.length() == 0) {
                    // 空结果直接忽略
                    return;
                }
                final long now = System.currentTimeMillis();

                int n = wr.results.length();
                for (int i = 0; i < n; i++) {
                    WorkerTaskResult r = (WorkerTaskResult) wr.results.get_at(i);
                    if (r == null) continue;

                    final String reqId    = r.request_id;
                    final String clientId = r.client_id;
                    final String key      = dedupKey(wr.batch_id, reqId, r.task_id);

                    // 1) 服务端去重：仅首次处理
                    Long prev = SEEN.putIfAbsent(key, now);
                    if (prev != null){
                        System.out.printf("[Spliter] dedup drop: req=%s batch=%s task=%s key=%s%n",
                                reqId, wr.batch_id, r.task_id, key);
                        continue;
                    }


                    // 2) 获取/初始化该 request 的状态
                    ReqState st = REQ.compute(reqId, (k, cur) -> (cur == null) ? new ReqState(reqId, clientId, now) : cur);

                    System.out.printf("[Spliter] recv ok: req=%s batch=%s task=%s buf=%d%n",
                            reqId, wr.batch_id, r.task_id, st.buffer.size());

                    // 已关闭（空闲超时），把后续视为“迟到”并丢弃
                    if (st.closed) continue;

                    // 3) 写入微批缓冲
                    ResultItem it = toItem(r);  // 注意：Bytes 若会被上游复用，可在此复制一份
                    synchronized (st) {
                        if (st.clientId == null && clientId != null && !clientId.isEmpty()) {
                            st.clientId = clientId;
                        }
                        st.buffer.add(it);
                        st.lastActivityTs = now;
                        if (MAX_BYTES_PER_UPDATE > 0) st.approxBytes += approxBytes(it);

                        // 条数或字节阈值到达时立即 flush
                        boolean byCount = st.buffer.size() >= MAX_ITEMS_PER_UPDATE;
                        boolean byBytes = (MAX_BYTES_PER_UPDATE > 0) && (st.approxBytes >= MAX_BYTES_PER_UPDATE);
                        if (byCount || byBytes) {
                            flushReq(st, now);
                        }
                    }
                }

            } catch (Throwable t) {
                System.err.println("[Spliter] on_process_sample error: " + t.getMessage());
                t.printStackTrace();
            }
        }

        /** 定时：检查应 flush 的请求，以及空闲超时关闭（只走空闲路径，不做绝对TTL） */
        private void flushDueRequests() {
            final long now = System.currentTimeMillis();
            // 限制每次扫描量，避免长时间占用
            final int SCAN_LIMIT = 50_000;
            int scanned = 0;

            for (Map.Entry<String, ReqState> e : REQ.entrySet()) {
                if (++scanned > SCAN_LIMIT) break;
                ReqState st = e.getValue();
                if (st == null) continue;

                synchronized (st) {
                    if (!st.closed) {
                        // 达到微批最大等待时间则 flush
                        if (!st.buffer.isEmpty() && (now - st.lastFlushTs) >= MAX_WAIT_MS) {
                            flushReq(st, now);
                        }
                        // 空闲超时：关闭（关闭前若有缓冲先 flush）
                        if ((now - st.lastActivityTs) > IDLE_CLOSE_MS) {
                            if (!st.buffer.isEmpty()) flushReq(st, now);
                            st.closed = true;
                        }
                    }
                }
            }
        }
        private void flushDueRequestsSafe() {
            try { flushDueRequests(); } catch (Throwable t) {
                System.err.println("[Spliter] flushDueRequests crashed: " + t);
                t.printStackTrace();
            }
        }

        /** 执行一次 flush（构建 ResultUpdate 并写出） */
        private void flushReq(ReqState st, long now){
            if (st.buffer.isEmpty()) {
                st.lastFlushTs = now;
                return;
            }

            ResultUpdate upd = new ResultUpdate();
            upd.request_id = st.requestId;
            upd.client_id  = st.clientId;

            int m = st.buffer.size();
            ResultItemSeq seq = new ResultItemSeq();
            seq.ensure_length(m, m);
            for (int i = 0; i < m; i++) seq.set_at(i, st.buffer.get(i));
            upd.items = seq;

            ReturnCode_t rc = updateWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Spliter] write ResultUpdate failed: " + rc + " req=" + st.requestId + " items=" + m);
            } else {
                 System.out.println("[Spliter] flush req=" + st.requestId + " items=" + m);
            }

            st.buffer.clear();
            st.approxBytes = 0;
            st.lastFlushTs = now;
        }

        /** 去重表清理（ConcurrentHashMap 的安全删除） */
        private void cleanupDedup() {
            final long now = System.currentTimeMillis();
            final long ttl = Math.max(10_000L, DEDUP_TTL_MS);
            final int  SCAN_LIMIT = 50_000;

            int scanned = 0, removed = 0;
            // 先收集再按 (key, expectedValue) 安全删除，避免并发误删
            List<Map.Entry<String, Long>> pend = new ArrayList<>(1024);

            for (Map.Entry<String, Long> e : SEEN.entrySet()) {
                if (++scanned > SCAN_LIMIT) break;
                if (now - e.getValue() > ttl) pend.add(e);
            }
            for (Map.Entry<String, Long> e : pend) {
                if (SEEN.remove(e.getKey(), e.getValue())) removed++;
            }
            if (removed > 0) {
                System.out.println("[Spliter] dedup cleanup removed=" + removed + " scanned=" + scanned);
            }
        }

        /** 清理已关闭或长时间无活动的请求状态 */
        private void cleanupReqStates() {
            final long now = System.currentTimeMillis();
            final int  SCAN_LIMIT = 20_000;
            int scanned = 0, removed = 0;

            Iterator<Map.Entry<String, ReqState>> it = REQ.entrySet().iterator();
            while (it.hasNext() && scanned < SCAN_LIMIT) {
                Map.Entry<String, ReqState> e = it.next();
                scanned++;
                ReqState st = e.getValue();
                // 已关闭，或极端情况下长久无活动
                if (st == null) { it.remove(); removed++; continue; }
                if (st.closed || (now - st.lastActivityTs) > (IDLE_CLOSE_MS * 10)) {
                    it.remove();
                    removed++;
                }
            }
            if (removed > 0) {
                System.out.println("[Spliter] req-state cleanup removed=" + removed + " scanned=" + scanned);
            }
        }

        public void shutdown() {
            if (sched != null && !sched.isShutdown()) {
                sched.shutdownNow();
            }
        }

        @Override
        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
            // 不需要
        }

        /* ===================== 小工具 ===================== */

        private static String dedupKey(String batchId, String reqId, String taskId) {
            String b = (batchId == null) ? "" : batchId;
            String r = (reqId   == null) ? "" : reqId;
            String t = (taskId  == null) ? "" : taskId;
            // 预估容量减少扩容
            StringBuilder sb = new StringBuilder(b.length() + r.length() + t.length() + 8);
            sb.append(b).append('|').append(r).append('|').append(t);
            return sb.toString();
        }

        private static ResultItem toItem(WorkerTaskResult r) {
            ResultItem it = new ResultItem();

            // String 类型保持引用即可
            it.task_id = r.task_id;
            it.status  = r.status;

            // ======= 拷贝 output_blob (ByteSeq) =======
            if (r.output_blob != null && r.output_blob.value != null) {
                ByteSeq src = r.output_blob.value;
                ByteSeq dst = new ByteSeq();
                int len = src.length();
                dst.ensure_length(len, len);
                for (int i = 0; i < len; i++) {
                    dst.set_at(i, src.get_at(i));
                }
                Bytes b = new Bytes();
                b.value = dst;
                it.output_blob = b;
            } else {
                it.output_blob = null;
            }

            // ======= 拷贝 texts (StringSeq) =======
            if (r.texts != null && r.texts.length() > 0) {
                StringSeq src = new StringSeq();
                src.ensure_length(r.texts.length(), r.texts.length());
                for (int i = 0; i < r.texts.length(); i++) {
                    src.set_at(i, r.texts.get_at(i));
                }
                it.texts = src;
            } else {
                it.texts = null;
            }

            return it;
        }



        private static int approxBytes(ResultItem it){
            int s = 0;
            if (it.task_id != null) s += it.task_id.length();
            if (it.status  != null) s += it.status.length();
            // 如需基于 Bytes 估算，请根据你的 Bytes 结构自行补充
            return s;
        }
    }

    /* ===================== Main ===================== */
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
            DataWriterQos wq = new DataWriterQos();
            pub.get_default_datawriter_qos(wq);
            wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
            wq.history.kind = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
            wq.history.depth = 2;

            ResultUpdateDataWriter upWriter = (ResultUpdateDataWriter)
                    pub.create_datawriter(upTopic, wq, null, StatusKind.STATUS_MASK_NONE);
            if (upWriter == null) {
                System.err.println("ResultUpdate writer creation failed");
                return;
            }

            DataReaderQos rq = new DataReaderQos();
            sub.get_default_datareader_qos(rq);
            rq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
            rq.history.kind = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
            rq.history.depth = 100;

            WorkerResultDataReader wrReader = (WorkerResultDataReader)
                    sub.create_datareader(wrTopic, rq, null, StatusKind.STATUS_MASK_NONE);
            if (wrReader == null) {
                System.err.println("WorkerResult reader creation failed");
                return;
            }

            // 6) Listener
            listener = new WorkerResultListener(upWriter);
            wrReader.set_listener(listener, StatusKind.DATA_AVAILABLE_STATUS);

            System.out.println("==================================================");
            System.out.println("Spliter started (server-side dedup + micro-batching + idle-timeout drop)");
            System.out.println("Microbatch: maxItems=" + MAX_ITEMS_PER_UPDATE + ", maxWaitMs=" + MAX_WAIT_MS +
                    ", maxBytes=" + MAX_BYTES_PER_UPDATE);
            System.out.println("Idle-close: " + IDLE_CLOSE_MS + " ms");
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
