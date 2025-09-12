package patcher;

import com.zrdds.infrastructure.*;
import com.zrdds.publication.DataWriterQos;
import data_structure.*; // InferenceRequest / OpenBatch / Claim / Grant / TaskList

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;
import com.zrdds.topic.Topic;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;
import com.zrdds.subscription.DataReaderQos;

/**
 * Patcher —— 方案B的协调层（announce → claim → grant → assigned TaskList）
 *
 * 责任：
 *  - 接收 InferenceRequest：丢给 TaskClassifier 做【按模型打批 → OpenBatch】。
 *  - 接收 Claim（包含 queue_length）：在一个很短的收集窗口内择优（queue_length 最小者）授予 Grant。
 *  - 授予后调用 TaskClassifier.onGrant(grant)：发布带 assigned_worker_id 的 TaskList（独占给胜者）。
 *

 */
public class Patcher {

    private static final int DOMAIN_ID = 100;

    // ==== 按实际工程替换这些 Topic 名 ====
    private static final String TOPIC_INFER_REQ   = "inference/request";
    private static final String TOPIC_OPEN_BATCH  = "inference/open_batch";
    private static final String TOPIC_CLAIM       = "inference/claim";
    private static final String TOPIC_GRANT       = "inference/grant";
    private static final String TOPIC_TASK_LIST   = "inference/task_list";

    /* ===================== Emitter contracts ===================== */
    /** 发布 OpenBatch（对接你的 DDS Writer） */
    public interface OpenBatchEmitter { void emit(OpenBatch ob); }
    /** 发布 TaskList（对接你的 DDS Writer） */
    public interface TaskListEmitter  { void emit(TaskList tl); }
    /** 发布 Grant（对接你的 DDS Writer） */
    public interface GrantEmitter     { void emit(Grant g); }

    /* ===================== Config ===================== */
    public static class Config extends TaskClassifier.Config {
        /** OpenBatch 的状态保留时长（毫秒），超时清理，避免内存泄漏。 */
        public long openBatchRetentionMs = 60_000L;
        /** 后台清理周期（毫秒）。 */
        public long housekeeperPeriodMs   = 1_000L;
        /** 同一批次的 Claim 收集窗口（毫秒）。0 表示来一条就立即授予。 */
        public long claimSelectWaitMs     = 2L;
    }

    /* ===================== State ===================== */
    private final Config cfg;
    private final OpenBatchEmitter openBatchEmitter;
    private final GrantEmitter grantEmitter;
    private final TaskClassifier classifier;

    /** 尚未授予的 open 批（batch_id → 状态） */
    private final ConcurrentMap<String, BatchState> open = new ConcurrentHashMap<>();
    /** 同一 batch 的 Claim 收集器（batch_id → 累加器） */
    private final ConcurrentMap<String, ClaimAccumulator> accums = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
            2, r -> { Thread t = new Thread(r, "patcher-scheduler"); t.setDaemon(true); return t; });

    /* ===================== Internal Types ===================== */
    private static final class BatchState {
        final String batchId;
        final String modelId;
        final long createTs;
        final AtomicReference<String> winner = new AtomicReference<>(null);
        BatchState(String batchId, String modelId, long createTs) {
            this.batchId = batchId; this.modelId = modelId; this.createTs = createTs;
        }
    }

    private static final class ClaimAccumulator {
        final String batchId;
        final List<Claim> claims = new ArrayList<>();
        ScheduledFuture<?> selectTask; // 选择任务（在 claimSelectWaitMs 之后触发一次）
        ClaimAccumulator(String batchId) { this.batchId = batchId; }
        synchronized void add(Claim c){ claims.add(c); }
        synchronized List<Claim> snapshot(){ return new ArrayList<>(claims); }
        synchronized void clear(){ claims.clear(); }
    }

    /* ===================== Construction ===================== */
    public Patcher(Config cfg,
                   OpenBatchEmitter openBatchWriter,
                   TaskListEmitter taskListWriter,
                   GrantEmitter grantWriter) {
        this.cfg = cfg;
        this.openBatchEmitter = openBatchWriter;
        this.grantEmitter = grantWriter;

        // 包装 TaskClassifier 的两个 emitter：
        //  1) OpenBatch：先登记到 open，再对外发布
        //  2) TaskList：直接对外发布（在 onGrant 调用时触发）
        this.classifier = new TaskClassifier(
                cfg,
                ob -> { onOpenBatch(ob); openBatchEmitter.emit(ob); },
                tl -> { taskListWriter.emit(tl); }
        );

        long p = Math.max(200L, cfg.housekeeperPeriodMs);
        scheduler.scheduleAtFixedRate(this::cleanupOpenBatches, p, p, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        classifier.shutdown();
        scheduler.shutdownNow();
        open.clear();
        accums.clear();
    }

    /* ===================== External entry points ===================== */

    /** Client 请求入口：交给 TaskClassifier 做按模型打批。 */
    public void onInferenceRequest(InferenceRequest req) {
        classifier.offer(req);
    }

    /**
     * Worker 的 Claim 入口：短暂收集，同一批次择优（queue_length 最小）授予 Grant。
     * 若 cfg.claimSelectWaitMs == 0，会立即调度 selectAndGrant。
     */
    public void onClaim(Claim c) {
        if (c == null) return;
        final String bid = c.batch_id;
        if (bid == null) return;

        final String wid = c.worker_id;
        final Integer qlen = c.queue_length;

        BatchState st = open.get(bid);
        if (st == null) return;

        ClaimAccumulator acc = accums.computeIfAbsent(c.batch_id, ClaimAccumulator::new);
        synchronized (acc) {
            // 放“拷贝”进去，避免后面被 DDS 改写
            Claim copy = new Claim();
            copy.batch_id = bid;
            copy.worker_id = wid;
            copy.queue_length = qlen;
            acc.add(copy);

            if (acc.selectTask == null) {
                long wait = waitWindowMsByPriority();
                // 只闭包捕获“不可变的 bid”
                acc.selectTask = scheduler.schedule(() -> selectAndGrant(bid), wait, TimeUnit.MILLISECONDS);
                // 可选：日志
                // System.out.println("[Patcher] scheduled select for " + bid);
            }
        }
    }

    /* ===================== Internal helpers ===================== */

    private void onOpenBatch(OpenBatch ob) {
        long now = System.currentTimeMillis();
        // 记录 open 状态（Task 内容由 TaskClassifier 内部持有，不重复存储）
        open.put(ob.batch_id, new BatchState(ob.batch_id, ob.model_id, now));
    }

    /** 在收集窗口结束后，选择 queue_length 最小的 claim，并授予 Grant。 */
    private void selectAndGrant(String batchId) {
        ClaimAccumulator acc = accums.remove(batchId);
        if (acc == null) return;

        BatchState st = open.get(batchId);
        if (st == null) return; // 可能已过期或已授予

        final List<Claim> list;
        synchronized (acc) {
            list = acc.snapshot(); // 到达顺序 = list 顺序
            acc.clear();
        }
        if (list.isEmpty()) return;

        final int priority = cfg.priority;

        Claim best;
        if (priority == 1) {
            // p=1：不打批，直接第一条
            best = pickByMinQueue(list);
        } else if (priority == 0) {
            // p=0：GPU优先 → 队列最小 → 先到先得
            int bestIdx = 0;
            int bestDevRank = devRankGpuFirst(list.get(0).worker_id);
            int bestQ = safeQ(list.get(0).queue_length);
            for (int i = 1; i < list.size(); i++) {
                Claim x = list.get(i);
                int r = devRankGpuFirst(x.worker_id);
                int q = safeQ(x.queue_length);
                if (r < bestDevRank || (r == bestDevRank && q < bestQ)) {
                    bestIdx = i; bestDevRank = r; bestQ = q;
                }
            }
            best = list.get(bestIdx);
        } else {
            // p=2：不区分设备 → 队列最小 → 先到先得
            int bestIdx = 0;
            int bestQ = safeQ(list.get(0).queue_length);
            for (int i = 1; i < list.size(); i++) {
                int q = safeQ(list.get(i).queue_length);
                if (q < bestQ) { bestIdx = i; bestQ = q; }
            }
            best = list.get(bestIdx);
        }

        // 原子授予，防并发双赢
        if (st.winner.compareAndSet(null, best.worker_id)) {
            Grant g = new Grant();
            g.batch_id = batchId;
            g.winner_worker_id = best.worker_id;

            try { grantEmitter.emit(g); } catch (Exception e) { e.printStackTrace(); }

            // 通知 TaskClassifier 发放 TaskList（assigned_worker_id = winner）
            classifier.onGrant(g);

            // 清理 open 状态
            open.remove(batchId);
        }
    }

    /** GPU优先的设备 rank：gpu(0) < cpu(1) < unknown(2) */
    private static int devRankGpuFirst(String workerId){
        String d = parseDevice(workerId);
        if ("gpu".equals(d)) return 0;
        if ("cpu".equals(d)) return 1;
        return 2;
    }

    /** 队列长度的安全读取：空值视为极大 */
    private static int safeQ(Integer q){
        return q == null ? Integer.MAX_VALUE : q;
    }

    /** 定时清理过期的 open 批，避免内存滞留。 */
    private void cleanupOpenBatches() {
        long now = System.currentTimeMillis();
        long ttl = Math.max(5_000L, cfg.openBatchRetentionMs);
        for (Map.Entry<String, BatchState> e : open.entrySet()) {
            BatchState st = e.getValue();
            if (now - st.createTs > ttl) {
                open.remove(e.getKey());
                accums.remove(e.getKey());
            }
        }
    }

    private long waitWindowMsByPriority(){
        int p = cfg.priority;
        if (p == 0) return Math.max(0L, cfg.waitFastMs);
        if (p == 1) return Math.max(0L, cfg.waitNoneMs);
        return Math.max(0L, cfg.defaultMaxWaitMs); // p==2 或其它
    }

    private static String parseDevice(String workerId){
        if (workerId == null) return "unknown";
        String id = workerId.toLowerCase(Locale.ROOT);
        if (id.endsWith("-gpu")) return "gpu";
        if (id.endsWith("-cpu")) return "cpu";
        return "unknown";
    }

    private static Claim pickByMinQueue(List<Claim> claims){
        int bestIdx = 0;
        int bestQ = safeQ(claims.get(0).queue_length);
        for (int i = 1; i < claims.size(); i++) {
            int q = safeQ(claims.get(i).queue_length);
            if (q < bestQ) { bestIdx = i; bestQ = q; }
        }
        return claims.get(bestIdx);
    }


    public static void main(String[] args) throws Exception {
        DomainParticipant dp = null;
        Publisher pub = null;
        Subscriber sub = null;

        InferenceRequestDataReader reqReader = null;
        ClaimDataReader claimReader = null;

        OpenBatchDataWriter openWriter = null;
        TaskListDataWriter taskWriter = null;
        GrantDataWriter grantWriter = null;

        Patcher patcher = null;

        try {
            // 1) Participant
            DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
            if (dpf == null) { System.err.println("DomainParticipantFactory.get_instance() failed"); return; }
            dp = dpf.create_participant(DOMAIN_ID,
                    DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (dp == null) { System.err.println("create_participant failed"); return; }

            // 2) 注册类型
            if (InferenceRequestTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || OpenBatchTypeSupport.get_instance().register_type(dp, null)        != ReturnCode_t.RETCODE_OK
                    || ClaimTypeSupport.get_instance().register_type(dp, null)            != ReturnCode_t.RETCODE_OK
                    || GrantTypeSupport.get_instance().register_type(dp, null)            != ReturnCode_t.RETCODE_OK
                    || TaskListTypeSupport.get_instance().register_type(dp, null)         != ReturnCode_t.RETCODE_OK) {
                System.err.println("register_type failed");
                return;
            }

            // 3) Topics
            Topic reqTopic   = dp.create_topic(TOPIC_INFER_REQ,
                    InferenceRequestTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic openTopic  = dp.create_topic(TOPIC_OPEN_BATCH,
                    OpenBatchTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic claimTopic = dp.create_topic(TOPIC_CLAIM,
                    ClaimTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic grantTopic = dp.create_topic(TOPIC_GRANT,
                    GrantTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic taskTopic  = dp.create_topic(TOPIC_TASK_LIST,
                    TaskListTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

            if (reqTopic == null || openTopic == null || claimTopic == null || grantTopic == null || taskTopic == null) {
                System.err.println("create_topic failed");
                return;
            }

            // 4) Pub/Sub
            pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (pub == null || sub == null) { System.err.println("create_publisher/subscriber failed"); return; }

            DataWriterQos wq = new DataWriterQos();
            pub.get_default_datawriter_qos(wq);
            wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
            wq.history.kind = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
            wq.history.depth = 2;

            // 5) Writers
            openWriter  = (OpenBatchDataWriter)  pub.create_datawriter(openTopic,  wq, null, StatusKind.STATUS_MASK_NONE);
            taskWriter  = (TaskListDataWriter)   pub.create_datawriter(taskTopic,  wq, null, StatusKind.STATUS_MASK_NONE);
            grantWriter = (GrantDataWriter)      pub.create_datawriter(grantTopic, wq, null, StatusKind.STATUS_MASK_NONE);
            if (openWriter == null || taskWriter == null || grantWriter == null) { System.err.println("create_datawriter failed"); return; }

            // 6) Patcher 实例（把 writer 封装进 emitter）
            final OpenBatchDataWriter fOpenW = openWriter;
            final TaskListDataWriter  fTaskW = taskWriter;
            final GrantDataWriter     fGrantW= grantWriter;

            Patcher.Config cfg = new Patcher.Config();
            patcher = new Patcher(
                    cfg,
                    ob -> { ReturnCode_t rc = fOpenW.write(ob, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[PatcherMain] openBatch write rc=" + rc); },
                    tl -> { ReturnCode_t rc = fTaskW.write(tl, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[PatcherMain] taskList write rc=" + rc); },
                    g  -> { ReturnCode_t rc = fGrantW.write(g, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[PatcherMain] grant write rc=" + rc); }
            );

            final Patcher fPatcher = patcher;

            DataReaderQos rq = new DataReaderQos();
            sub.get_default_datareader_qos(rq);
            rq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
            rq.history.kind = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
            rq.history.depth = 100;

            // 7) Readers + Listeners
            reqReader = (InferenceRequestDataReader) sub.create_datareader(
                    reqTopic, rq, null, StatusKind.STATUS_MASK_NONE);
            claimReader = (ClaimDataReader) sub.create_datareader(
                    claimTopic, rq, null, StatusKind.STATUS_MASK_NONE);
            if (reqReader == null || claimReader == null) { System.err.println("create_datareader failed"); return; }

            // InferenceRequest → onInferenceRequest
            reqReader.set_listener(new SimpleDataReaderListener<InferenceRequest, InferenceRequestSeq, InferenceRequestDataReader>() {
                @Override
                public void on_data_arrived(DataReader dataReader, Object o, SampleInfo sampleInfo) {

                }

                @Override
                public void on_process_sample(DataReader reader, InferenceRequest sample, SampleInfo info) {
                    if (sample == null || info == null || !info.valid_data) return;
                    try {
                        System.out.println("[Patcher] got InferenceRequest: request_id=" + sample.request_id
                                + " tasks=" + (sample.tasks==null?0:sample.tasks.length()));
                        fPatcher.onInferenceRequest(sample);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }, StatusKind.DATA_AVAILABLE_STATUS /* 或 STATUS_MASK_ALL 更稳 */);


            // Claim → onClaim
            // Claim → onClaim
            claimReader.set_listener(new SimpleDataReaderListener<Claim, ClaimSeq, ClaimDataReader>() {
                @Override
                public void on_data_arrived(DataReader dataReader, Object o, SampleInfo sampleInfo) {

                }

                @Override
                public void on_process_sample(DataReader reader, Claim sample, SampleInfo info) {
                    if (sample == null || info == null || !info.valid_data) return;
                    try {
                        System.out.println("[Patcher] got Claim: batch_id=" + sample.batch_id
                                + " worker_id=" + sample.worker_id + " qlen=" + sample.queue_length);
                        fPatcher.onClaim(sample);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }, StatusKind.DATA_AVAILABLE_STATUS /* 或 STATUS_MASK_ALL */);



            System.out.println("==================================================");
            System.out.println("Patcher started.");
            System.out.println("Sub: " + TOPIC_INFER_REQ + ", " + TOPIC_CLAIM);
            System.out.println("Pub: " + TOPIC_OPEN_BATCH + ", " + TOPIC_GRANT + ", " + TOPIC_TASK_LIST);
            System.out.println("Press ENTER to exit...");
            System.out.println("==================================================");
//            SubscriptionMatchedStatus st = new  SubscriptionMatchedStatus();
//                try {
//                    while (true) {
//                        ReturnCode_t rc = reqReader.get_subscription_matched_status(st);
//                        if (rc != ReturnCode_t.RETCODE_OK) {
//                            System.out.println("get_publication_matched_status rc=" + rc);
//                            break;
//                        }
//                        System.out.println("PUB matched: current=" + st.current_count +
//                                " total=" + st.total_count +
//                                " Δ=" + st.current_count_change +
//                                " lastSub=" + (st.last_publication_handle != null ?
//                                st.last_publication_handle.value : 0));
//
//                        if (st.current_count > 0) {
//                            System.out.println("已匹配到 Reader，可以安全发送数据");
//                            break;
//                        }
//                        Thread.sleep(200); // 200ms 间隔轮询
//                    }
//                } catch (InterruptedException ignored) {}

            System.in.read();

        } finally {
            try { if (patcher != null) patcher.shutdown(); } catch (Throwable ignored) {}
            try { DDSIF.Finalize(); } catch (Throwable ignored) {}
            System.out.println("Patcher stopped.");
        }
    }
}
