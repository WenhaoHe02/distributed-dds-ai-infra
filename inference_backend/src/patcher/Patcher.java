package patcher;

import com.zrdds.infrastructure.*;
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

/**
 * Patcher — announce → claim → grant → assigned TaskList
 *
 * - OpenBatch is unchanged; we parse priority p from batch_id "b-p{p}-...".
 * - Claim collection window length depends on this batch's priority.
 * - Winner selection only uses worker_id (GPU/CPU string + queue length).
 */
public class Patcher {

    private static final int DOMAIN_ID = 100;

    private static final String TOPIC_INFER_REQ   = "inference/request";
    private static final String TOPIC_OPEN_BATCH  = "inference/open_batch";
    private static final String TOPIC_CLAIM       = "inference/claim";
    private static final String TOPIC_GRANT       = "inference/grant";
    private static final String TOPIC_TASK_LIST   = "inference/task_list";

    /* ===================== Emitter contracts ===================== */
    public interface OpenBatchEmitter { void emit(OpenBatch ob); }
    public interface TaskListEmitter  { void emit(TaskList tl); }
    public interface GrantEmitter     { void emit(Grant g); }

    /* ===================== Config ===================== */
    public static class Config extends TaskClassifier.Config {
        public long openBatchRetentionMs = 60_000L;
        public long housekeeperPeriodMs  = 1_000L;
        // Inherit waitFastMs / waitNoneMs / defaultMaxWaitMs from TaskClassifier.Config
    }

    /* ===================== State ===================== */
    private final Config cfg;
    private final OpenBatchEmitter openBatchEmitter;
    private final GrantEmitter grantEmitter;
    private final TaskClassifier classifier;

    private final ConcurrentMap<String, BatchState> open = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ClaimAccumulator> accums = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
            2, r -> { Thread t = new Thread(r, "patcher-scheduler"); t.setDaemon(true); return t; });

    /* ===================== Internal Types ===================== */
    private static final class BatchState {
        final String batchId, modelId; final long createTs; final int priority; // 0/1/2
        final AtomicReference<String> winner = new AtomicReference<>(null);
        BatchState(String batchId, String modelId, long createTs, int priority) {
            this.batchId=batchId; this.modelId=modelId; this.createTs=createTs; this.priority=priority;
        }
    }

    private static final class ClaimAccumulator {
        final String batchId;
        final List<Claim> claims = new ArrayList<>();
        ScheduledFuture<?> selectTask;
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

    public void onInferenceRequest(InferenceRequest req) {
        classifier.offer(req); // TaskClassifier 会从 req.request_id 解析 priority
    }

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
            Claim copy = new Claim();
            copy.batch_id = bid; copy.worker_id = wid; copy.queue_length = qlen;
            acc.add(copy);

            if (acc.selectTask == null) {
                long wait = waitWindowMsByPriority(st.priority);
                acc.selectTask = scheduler.schedule(() -> selectAndGrant(bid), wait, TimeUnit.MILLISECONDS);
            }
        }
    }

    /* ===================== Internal helpers ===================== */

    private void onOpenBatch(OpenBatch ob) {
        long now = System.currentTimeMillis();
        int pri = parsePriorityFromBatchId(ob.batch_id); // b-p{p}-...
        open.put(ob.batch_id, new BatchState(ob.batch_id, ob.model_id, now, pri));
    }

    private void selectAndGrant(String batchId) {
        ClaimAccumulator acc = accums.remove(batchId);
        if (acc == null) return;

        BatchState st = open.get(batchId);
        if (st == null) return;

        final List<Claim> list;
        synchronized (acc) {
            list = acc.snapshot();
            acc.clear();
        }
        if (list.isEmpty()) return;

        final int priority = st.priority;

        Claim best;
        if (priority == 1) {
            best = pickByMinQueue(list);              // no-wait: shortest queue
        } else if (priority == 0) {
            best = pickGpuFirstThenMinQueue(list);    // gpu preferred -> shortest queue
        } else {
            best = pickByMinQueue(list);              // default
        }

        if (st.winner.compareAndSet(null, best.worker_id)) {
            Grant g = new Grant();
            g.batch_id = batchId;
            g.winner_worker_id = best.worker_id;

            try { grantEmitter.emit(g); } catch (Exception e) { e.printStackTrace(); }
            classifier.onGrant(g);
            open.remove(batchId);
        }
    }

    private static int safeQ(Integer q){ return q == null ? Integer.MAX_VALUE : q; }

    private static Claim pickByMinQueue(List<Claim> claims){
        int bestIdx = 0; int bestQ = safeQ(claims.get(0).queue_length);
        for (int i = 1; i < claims.size(); i++) {
            int q = safeQ(claims.get(i).queue_length);
            if (q < bestQ) { bestIdx = i; bestQ = q; }
        }
        return claims.get(bestIdx);
    }

    private static Claim pickGpuFirstThenMinQueue(List<Claim> claims){
        int idx = 0, bestDev = devRankGpuFirst(claims.get(0).worker_id);
        int bestQ = safeQ(claims.get(0).queue_length);
        for (int i=1;i<claims.size();i++){
            Claim x = claims.get(i);
            int d = devRankGpuFirst(x.worker_id);
            int q = safeQ(x.queue_length);
            if (d < bestDev || (d==bestDev && q<bestQ)){
                idx = i; bestDev=d; bestQ=q;
            }
        }
        return claims.get(idx);
    }

    private static int devRankGpuFirst(String workerId){
        String d = parseDevice(workerId);
        if ("gpu".equals(d)) return 0;
        if ("cpu".equals(d)) return 1;
        return 2;
    }
    private static String parseDevice(String workerId){
        if (workerId == null) return "unknown";
        String id = workerId.toLowerCase(Locale.ROOT);
        if (id.endsWith("-gpu") || id.contains("gpu")) return "gpu";
        if (id.endsWith("-cpu") || id.contains("cpu")) return "cpu";
        return "unknown";
    }

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

    private long waitWindowMsByPriority(int p){
        if (p == 0) return Math.max(0L, cfg.waitFastMs);
        if (p == 1) return Math.max(0L, cfg.waitNoneMs);
        return Math.max(0L, cfg.defaultMaxWaitMs);
    }

    /** Parse p from batch_id "b-p{p}-..." */
    private static int parsePriorityFromBatchId(String batchId){
        if (batchId == null) return 2;
        try {
            int i = batchId.indexOf("-p");
            if (i >= 0) {
                int j = batchId.indexOf('-', i + 2);
                String s = (j > i) ? batchId.substring(i + 2, j) : batchId.substring(i + 2);
                int p = Integer.parseInt(s);
                return (p==0 || p==1) ? p : 2;
            }
        } catch (Exception ignored) {}
        return 2;
    }

    /* ===================== Example main (与你原来一致，仅日志微调) ===================== */

    public static void main(String[] args) throws Exception {
        DomainParticipant dp = null; Publisher pub = null; Subscriber sub = null;
        InferenceRequestDataReader reqReader = null; ClaimDataReader claimReader = null;
        OpenBatchDataWriter openWriter = null; TaskListDataWriter taskWriter = null; GrantDataWriter grantWriter = null;
        Patcher patcher = null;

        try {
            DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
            if (dpf == null) { System.err.println("DomainParticipantFactory.get_instance() failed"); return; }
            dp = dpf.create_participant(DOMAIN_ID,
                    DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (dp == null) { System.err.println("create_participant failed"); return; }

            if (InferenceRequestTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || OpenBatchTypeSupport.get_instance().register_type(dp, null)        != ReturnCode_t.RETCODE_OK
                    || ClaimTypeSupport.get_instance().register_type(dp, null)            != ReturnCode_t.RETCODE_OK
                    || GrantTypeSupport.get_instance().register_type(dp, null)            != ReturnCode_t.RETCODE_OK
                    || TaskListTypeSupport.get_instance().register_type(dp, null)         != ReturnCode_t.RETCODE_OK) {
                System.err.println("register_type failed");
                return;
            }

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
                System.err.println("create_topic failed"); return;
            }

            pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (pub == null || sub == null) { System.err.println("create_publisher/subscriber failed"); return; }

            openWriter  = (OpenBatchDataWriter)  pub.create_datawriter(openTopic,  Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            taskWriter  = (TaskListDataWriter)   pub.create_datawriter(taskTopic,  Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            grantWriter = (GrantDataWriter)      pub.create_datawriter(grantTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (openWriter == null || taskWriter == null || grantWriter == null) { System.err.println("create_datawriter failed"); return; }

            final OpenBatchDataWriter fOpenW  = openWriter;
            final TaskListDataWriter  fTaskW  = taskWriter;
            final GrantDataWriter     fGrantW = grantWriter;

            Patcher.Config cfg = new Patcher.Config();
            final Patcher fPatcher = new Patcher(
                    cfg,
                    ob -> { ReturnCode_t rc = fOpenW.write(ob, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[PatcherMain] openBatch write rc=" + rc); },
                    tl -> { ReturnCode_t rc = fTaskW.write(tl, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[PatcherMain] taskList write rc=" + rc); },
                    g  -> { ReturnCode_t rc = fGrantW.write(g, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[PatcherMain] grant write rc=" + rc); }
            );

            reqReader = (InferenceRequestDataReader) sub.create_datareader(
                    reqTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            claimReader = (ClaimDataReader) sub.create_datareader(
                    claimTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (reqReader == null || claimReader == null) { System.err.println("create_datareader failed"); return; }

            reqReader.set_listener(new SimpleDataReaderListener<InferenceRequest, InferenceRequestSeq, InferenceRequestDataReader>() {
                @Override public void on_data_arrived(DataReader dataReader, Object o, SampleInfo sampleInfo) { }
                @Override public void on_process_sample(DataReader reader, InferenceRequest sample, SampleInfo info) {
                    if (sample == null || info == null || !info.valid_data) return;
                    try {
                        int pr = TaskClassifier.extractPriority(sample.request_id);
                        System.out.println("[Patcher] got InferenceRequest: request_id=" + sample.request_id +
                                " (parsed priority=" + pr + ") tasks=" + (sample.tasks==null?0:sample.tasks.length()));
                        fPatcher.onInferenceRequest(sample);
                    } catch (Throwable t) { t.printStackTrace(); }
                }
            }, StatusKind.DATA_AVAILABLE_STATUS);

            claimReader.set_listener(new SimpleDataReaderListener<Claim, ClaimSeq, ClaimDataReader>() {
                @Override public void on_data_arrived(DataReader dataReader, Object o, SampleInfo sampleInfo) { }
                @Override public void on_process_sample(DataReader reader, Claim sample, SampleInfo info) {
                    if (sample == null || info == null || !info.valid_data) return;
                    try {
                        System.out.println("[Patcher] got Claim: batch_id=" + sample.batch_id +
                                " worker_id=" + sample.worker_id + " qlen=" + sample.queue_length);
                        fPatcher.onClaim(sample);
                    } catch (Throwable t) { t.printStackTrace(); }
                }
            }, StatusKind.DATA_AVAILABLE_STATUS);

            System.out.println("==================================================");
            System.out.println("Patcher started.");
            System.out.println("Sub: " + TOPIC_INFER_REQ + ", " + TOPIC_CLAIM);
            System.out.println("Pub: " + TOPIC_OPEN_BATCH + ", " + TOPIC_GRANT + ", " + TOPIC_TASK_LIST);
            System.out.println("Press ENTER to exit...");
            System.out.println("==================================================");

            System.in.read();

        } finally {
            try { if (patcher != null) patcher.shutdown(); } catch (Throwable ignored) {}
            try { DDSIF.Finalize(); } catch (Throwable ignored) {}
            System.out.println("Patcher stopped.");
        }
    }
}
