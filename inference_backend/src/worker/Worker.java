package worker;

import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.infrastructure.StatusKind;
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
import data_structure.*; // OpenBatch / Claim / TaskList / WorkerResult / WorkerTaskResult

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Worker (Scheme B, Claim updated: {batch_id, worker_id, queue_length})
 */
public class Worker {

    private static final int DOMAIN_ID = 100; // 补上

    private static final String TOPIC_OPEN_BATCH    = "inference/open_batch";
    private static final String TOPIC_CLAIM         = "inference/claim";
    private static final String TOPIC_TASK_LIST     = "inference/task_list";
    private static final String TOPIC_WORKER_RESULT = "inference/worker_result";

    /* ===================== Wiring contracts ===================== */
    public interface ClaimEmitter { void emit(Claim c); }
    public interface ResultEmitter { void emit(WorkerResult wr); }

    /* ===================== Config ===================== */
    public static class Config {
        public final String workerId;
        public final String modelId;
        public int maxInflightBatches = 2;
        public int queueCapacity = 64;
        public Config(String workerId, String modelId){ this.workerId = workerId; this.modelId = modelId; }
    }

    /* ===================== State ===================== */
    private final Config cfg;
    private final ClaimEmitter claimEmitter;
    private final ResultEmitter resultEmitter;
    private final ModelRunner runner; // 保存 runner 实例

    private final BlockingQueue<TaskList> batchQueue;
    private final AtomicInteger inflight = new AtomicInteger(0);

    private final LinkedHashMap<String, Boolean> claimedLRU = new LinkedHashMap<String, Boolean>(256, 0.75f, true){
        @Override protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest){ return size() > 4096; }
    };
    private final Set<String> seenTaskList = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    public Worker(Config cfg, ClaimEmitter claimEmitter, ResultEmitter resultEmitter, ModelRunner runner){
        this.cfg = cfg;
        this.claimEmitter = claimEmitter;
        this.resultEmitter = resultEmitter;
        this.runner = runner;
        this.batchQueue = new ArrayBlockingQueue<>(cfg.queueCapacity);
    }

    /* ===================== DDS listeners hook ===================== */
    public void onOpenBatch(OpenBatch ob){
        if (ob == null) return;
        if (!cfg.modelId.equals(ob.model_id)) return;
        if (currentDepth() >= cfg.maxInflightBatches) return;
        synchronized (claimedLRU){
            if (claimedLRU.containsKey(ob.batch_id)) return;
            claimedLRU.put(ob.batch_id, Boolean.TRUE);
        }
        Claim c = new Claim();
        c.batch_id = ob.batch_id;
        c.worker_id = cfg.workerId;
        c.queue_length = currentDepth();
        try { claimEmitter.emit(c); } catch (Exception e){ e.printStackTrace(); }
    }

    public void onTaskList(TaskList tl){
        if (tl == null) return;
        if (!cfg.workerId.equals(tl.assigned_worker_id)) return;
        if (!cfg.modelId.equals(tl.model_id)) return;
        if (!seenTaskList.add(tl.batch_id)) return;
        if (!batchQueue.offer(tl)){
            batchQueue.poll();
            batchQueue.offer(tl);
        }
    }

    /* ===================== Lifecycle ===================== */
    public void start(){
        if (running.getAndSet(true)) return;
        consumerThread = new Thread(this::consumeLoop, "worker-consumer-" + cfg.workerId);
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    public void stop(){
        running.set(false);
        if (consumerThread != null) consumerThread.interrupt();
    }

    /* ===================== Internals ===================== */
    private void consumeLoop(){
        while(running.get()){
            try {
                TaskList tl = batchQueue.poll(100, TimeUnit.MILLISECONDS);
                if (tl == null) continue;
                inflight.incrementAndGet();
                WorkerResult wr;
                try {
                    wr = ModelRunner.runBatchedTask(tl); // 用实例方法
                } catch (Throwable ex){
                    wr = synthesizeErrorResult(tl, ex);
                }
                wr.batch_id = tl.batch_id;
                wr.model_id = tl.model_id;
                wr.worker_id = cfg.workerId;
                try { resultEmitter.emit(wr); } catch (Exception e){ e.printStackTrace(); }
            } catch (InterruptedException ie){
                Thread.currentThread().interrupt();
            } finally {
                inflight.decrementAndGet();
            }
        }
    }

    private WorkerResult synthesizeErrorResult(TaskList tl, Throwable ex){
        WorkerResult wr = new WorkerResult();
        wr.batch_id = tl.batch_id;
        wr.model_id = tl.model_id;
        wr.worker_id = cfg.workerId;
        WorkerTaskResultSeq out = new WorkerTaskResultSeq();
        int n = tl.tasks == null ? 0 : tl.tasks.length();
        out.ensure_length(n, n);
        for (int i=0;i<n;i++){
            Task t = (Task) tl.tasks.get_at(i);
            WorkerTaskResult r = new WorkerTaskResult();
            r.request_id = t.request_id;
            r.task_id = t.task_id;
            r.client_id = t.client_id;
            r.status = "ERROR_RUNNER";
            r.output_blob = new Bytes();
            out.set_at(i, r);
        }
        wr.results = out;
        return wr;
    }

    public int currentDepth(){ return inflight.get() + batchQueue.size(); }

    public static void main(String[] args) throws Exception {
        String workerId = sysOrEnv("worker.id", "WORKER_ID", "worker-1");
        String modelId  = sysOrEnv("worker.model", "WORKER_MODEL", "model_0");

        DomainParticipant dp = null;
        Publisher pub = null;
        Subscriber sub = null;

        OpenBatchDataReader openReader = null;
        TaskListDataReader taskReader = null;
        ClaimDataWriter claimWriter = null;
        WorkerResultDataWriter resultWriter = null;

        Worker worker = null;

        try {
            DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
            if (dpf == null) { System.err.println("DomainParticipantFactory.get_instance() failed"); return; }
            dp = dpf.create_participant(DOMAIN_ID,
                    DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (dp == null) { System.err.println("create_participant failed"); return; }

            if (OpenBatchTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || ClaimTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || TaskListTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK
                    || WorkerResultTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
                System.err.println("register_type failed");
                return;
            }

            Topic openTopic = dp.create_topic(TOPIC_OPEN_BATCH,
                    OpenBatchTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic claimTopic = dp.create_topic(TOPIC_CLAIM,
                    ClaimTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic taskTopic = dp.create_topic(TOPIC_TASK_LIST,
                    TaskListTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            Topic resultTopic = dp.create_topic(TOPIC_WORKER_RESULT,
                    WorkerResultTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (openTopic == null || claimTopic == null || taskTopic == null || resultTopic == null) {
                System.err.println("create_topic failed"); return;
            }

            pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (pub == null || sub == null) { System.err.println("create_publisher/subscriber failed"); return; }

            claimWriter = (ClaimDataWriter) pub.create_datawriter(
                    claimTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            resultWriter = (WorkerResultDataWriter) pub.create_datawriter(
                    resultTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (claimWriter == null || resultWriter == null) { System.err.println("create_datawriter failed"); return; }

            // 让被 lambda 捕获的变量是 final
            final ClaimDataWriter claimW = claimWriter;
            final WorkerResultDataWriter resultW = resultWriter;

            Worker.Config cfg = new Worker.Config(workerId, modelId);
            worker = new Worker(
                    cfg,
                    c  -> { ReturnCode_t rc = claimW.write(c, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[WorkerMain] claim write rc=" + rc); },
                    wr -> { ReturnCode_t rc = resultW.write(wr, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        if (rc != ReturnCode_t.RETCODE_OK) System.err.println("[WorkerMain] result write rc=" + rc); },
                    new ModelRunner()
            );

            // 供匿名内部类使用的 final 引用
            final Worker fWorker = worker;

            openReader = (OpenBatchDataReader) sub.create_datareader(
                    openTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            taskReader = (TaskListDataReader) sub.create_datareader(
                    taskTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (openReader == null || taskReader == null) { System.err.println("create_datareader failed"); return; }

            openReader.set_listener(new SimpleDataReaderListener<OpenBatch, OpenBatchSeq, OpenBatchDataReader>() {
                @Override
                public void on_process_sample(DataReader reader, OpenBatch sample, SampleInfo info) {
                    if (sample == null) return;
                    try { fWorker.onOpenBatch(sample); } catch (Throwable t) { t.printStackTrace(); }
                }
                @Override
                public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
                    // 必须实现的抽象方法，留空即可
                }
            }, StatusKind.DATA_AVAILABLE_STATUS);

            taskReader.set_listener(new SimpleDataReaderListener<TaskList, TaskListSeq, TaskListDataReader>() {
                @Override
                public void on_process_sample(DataReader reader, TaskList sample, SampleInfo info) {
                    if (sample == null) return;
                    try { fWorker.onTaskList(sample); } catch (Throwable t) { t.printStackTrace(); }
                }
                @Override
                public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
                    // 必须实现的抽象方法，留空即可
                }
            }, StatusKind.DATA_AVAILABLE_STATUS);

            fWorker.start();

            System.out.println("==================================================");
            System.out.println("Worker started. worker_id=" + workerId + " model_id=" + modelId);
            System.out.println("Sub: " + TOPIC_OPEN_BATCH + ", " + TOPIC_TASK_LIST);
            System.out.println("Pub: " + TOPIC_CLAIM + ", " + TOPIC_WORKER_RESULT);
            System.out.println("Press ENTER to exit...");
            System.out.println("==================================================");

            System.in.read();

        } finally {
            try { if (worker != null) worker.stop(); } catch (Throwable ignored) {}
            try { DDSIF.Finalize(); } catch (Throwable ignored) {}
            System.out.println("Worker stopped.");
        }
    }

    private static String sysOrEnv(String sysKey, String envKey, String defVal) {
        String v = System.getProperty(sysKey);
        if (v == null || v.isEmpty()) v = System.getenv(envKey);
        return (v == null || v.isEmpty()) ? defVal : v;
    }
}
