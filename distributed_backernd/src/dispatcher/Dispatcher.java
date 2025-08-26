package dispatcher;

import com.zrdds.infrastructure.*;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;

import data_structure.*; // InferenceRequest/TaskList/WorkerResult/* + *TypeSupport/*Seq/*Reader/*Writer
import data_structure.Bytes;

public class Dispatcher {

    static final int DOMAIN_ID = 100;

    // Topics
    static final String TOPIC_REQUEST    = "inference/request";
    static final String TOPIC_TASKLIST   = "inference/tasklist";     // 仍然一个 Topic；Worker 只订阅自己 model
    static final String TOPIC_WORKER_RES = "inference/worker_result";
    static final String TOPIC_AGGREGATED = "inference/aggregated";   // 可选 ACK

    // writers
    private static TaskListDataWriter         taskWriter;
    private static AggregatedResultDataWriter ackWriter;             // 可为 null（不要 ACK 时）

    // classifier（跨请求聚合）
    private static TaskClassifier classifier;

    // ====== 收到 InferenceRequest：可选 ACK + 投喂给 TaskClassifier ======
    static final class ReqListener extends SimpleDataReaderListener<InferenceRequest, InferenceRequestSeq, InferenceRequestDataReader> {
        @Override
        public void on_process_sample(DataReader r, InferenceRequest req, SampleInfo info) {
            try {
                // 1) 可选：ACK
                if (ackWriter != null) {
                    AggregatedResult ack = new AggregatedResult();
                    ack.request_id = req.request_id;
                    if (req.input_blob != null && req.input_blob.length() > 0) {
                        SingleTask t0 = req.input_blob.get_at(0);
                        ack.client_id = (t0 != null && t0.client_id != null) ? t0.client_id : "";
                    } else {
                        ack.client_id = "";
                    }
                    ack.status = "RECEIVED";
                    ack.error_message = "";
                    ack.results = new SingleResultSeq();
                    ReturnCode_t rc = ackWriter.write(ack, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    System.out.println("[Dispatcher] ACK -> " + rc + " req=" + ack.request_id);
                }

                // 2) 投喂给聚合器（由聚合器跨请求凑批并发往 Worker）
                classifier.offer(req);

            } catch (Throwable t) { t.printStackTrace(); }
        }
        @Override public void on_data_arrived(DataReader r, Object s, SampleInfo i) {}
    }

    // ===== 打印 WorkerResult =====
    static final class WorkerResListener extends SimpleDataReaderListener<WorkerResult, WorkerResultSeq, WorkerResultDataReader> {
        @Override
        public void on_process_sample(DataReader r, WorkerResult wr, SampleInfo info) {
            try {
                int n = (wr.results != null) ? wr.results.length() : 0;
                System.out.println("[Dispatcher] ←Worker WorkerResult count=" + n);
                for (int i = 0; i < n; i++) {
                    SingleResult res = wr.results.get_at(i);
                    if (res == null) continue;
                    String reqId = (res.task != null && res.task.request_id != null) ? res.task.request_id : "";
                    String cliId = (res.task != null && res.task.client_id  != null) ? res.task.client_id  : "";
                    String taskId= (res.task != null && res.task.task_id    != null) ? res.task.task_id    : "";
                    String model = (res.task != null && res.task.model_id   != null) ? res.task.model_id   : "";
                    long   latMs = res.latency_ms;
                    String status= (res.status != null) ? res.status : "";
                    int outLen = (res.output_blob != null) ? res.output_blob.length() : 0;

                    System.out.println(String.format(
                            "  result[%d]: req=%s client=%s task=%s model=%s status=%s latency=%dms out_len=%d",
                            i, reqId, cliId, taskId, model, status, latMs, outLen));
                }
            } catch (Throwable t) { t.printStackTrace(); }
        }
        @Override public void on_data_arrived(DataReader r, Object s, SampleInfo i) {}
    }

    private static byte[] loadFirstJpgFromPackage(String packageName) {
        try {
            String path = packageName.replace('.', '/');
            ClassLoader cl = Dispatcher.class.getClassLoader();

            java.net.URL url = cl.getResource(path);
            if (url == null) {
                System.err.println("[Dispatcher] resource package not found: " + path);
                return null;
            }

            java.nio.file.Path dir = java.nio.file.Paths.get(url.toURI());
            try (java.util.stream.Stream<java.nio.file.Path> stream = java.nio.file.Files.list(dir)) {
                return stream
                        .filter(p -> {
                            String n = p.getFileName().toString().toLowerCase();
                            return n.endsWith(".jpg") || n.endsWith(".jpeg");
                        })
                        .map(p -> {
                            try {
                                System.out.println("[Dispatcher] picked image: " + p);
                                return java.nio.file.Files.readAllBytes(p);
                            } catch (Exception e) {
                                e.
                                        printStackTrace();
                                return null;
                            }
                        })
                        .filter(b -> b != null)
                        .findFirst()
                        .orElse(null);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void main(String[] args) throws Exception {
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        DomainParticipant dp = dpf.create_participant(
                DOMAIN_ID,
                DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null, StatusKind.STATUS_MASK_NONE);
        if (dp == null) { System.err.println("create dp failed"); return; }

        // register types
        InferenceRequestTypeSupport irTS = (InferenceRequestTypeSupport) InferenceRequestTypeSupport.get_instance();
        TaskListTypeSupport        tlTS = (TaskListTypeSupport)        TaskListTypeSupport.get_instance();
        WorkerResultTypeSupport    wrTS = (WorkerResultTypeSupport)    WorkerResultTypeSupport.get_instance();
        AggregatedResultTypeSupport agTS= (AggregatedResultTypeSupport) AggregatedResultTypeSupport.get_instance();
        if (irTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                tlTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                wrTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                agTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed"); return;
        }

        // topics
        Topic reqTopic = dp.create_topic(TOPIC_REQUEST,    irTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic tlTopic  = dp.create_topic(TOPIC_TASKLIST,   tlTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic wrTopic  = dp.create_topic(TOPIC_WORKER_RES, wrTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic ackTopic = dp.create_topic(TOPIC_AGGREGATED, agTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        Publisher  pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        taskWriter = (TaskListDataWriter)
                pub.create_datawriter(tlTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        ackWriter  = (AggregatedResultDataWriter)
                pub.create_datawriter(ackTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        InferenceRequestDataReader reqReader = (InferenceRequestDataReader)
                sub.create_datareader(reqTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        WorkerResultDataReader wrReader = (WorkerResultDataReader)
                sub.create_datareader(wrTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // === 初始化 TaskClassifier：按 model_id 聚合，跨请求凑批 ===
        TaskClassifier.Config cfg = new TaskClassifier.Config();
        cfg.maxBatch  = 1;    // 先不打批，如要小批可调成 4/8
        cfg.maxWaitMs = 0;    // 不等待
        // 若你想模型 A 打 8、模型 B 打 4： cfg.perModelMaxBatch.put("modelA", 8); cfg.perModelMaxBatch.put("modelB", 4);

        TaskClassifier.Emitter emitter = (modelId, tl) -> {
            ReturnCode_t rc = taskWriter.write(tl, InstanceHandle_t.HANDLE_NIL_NATIVE);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Dispatcher] write TaskList failed model=" + modelId + " rc=" + rc);
            } else {
                System.out.println("[Dispatcher] →Worker TaskList model=" + modelId + " n=" + tl.task_num);
            }
        };
        classifier = new TaskClassifier(cfg, emitter);

        // listeners
        reqReader.set_listener(new ReqListener(),      StatusKind.STATUS_MASK_ALL);
        wrReader.set_listener(new WorkerResListener(), StatusKind.STATUS_MASK_ALL);

        System.out.println("Dispatcher started. Press ENTER to exit.");
        System.in.read();

        if (true) {
            // 构造一个 SingleTask
            SingleTask t = new SingleTask();
            t.task_id = "task-test-1";
            t.request_id = "req-test-1";
            t.client_id = "cli-local";
            t.model_id = "model-x";
            // 加载第一张 jpg
            byte[] jpgBytes = loadFirstJpgFromPackage("resources.images");
            if (jpgBytes != null) {
                Bytes bb = new Bytes();
                bb.from_array(jpgBytes, jpgBytes.length);
                t.input_blob = bb;
            }

            // 打包成 TaskList
            TaskList tl = new TaskList();
            tl.tasks = new SingleTaskSeq();
            tl.tasks.ensure_length(1, 1);
            tl.tasks.set_at(0, t);
            tl.task_num = 1;
            tl.worker_id = "auto";
            tl.meta = new KVList();
            tl.meta.value = new KVSeq();

            ReturnCode_t rc = taskWriter.write(tl, InstanceHandle_t.HANDLE_NIL_NATIVE);
            System.out.println("[DispatcherTest] wrote TaskList rc=" + rc);
        }

        dp.delete_contained_entities();
        dpf.delete_participant(dp);
        DomainParticipantFactory.finalize_instance();
    }
}
