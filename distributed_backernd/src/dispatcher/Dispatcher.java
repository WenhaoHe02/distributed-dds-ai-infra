package dispatcher;
import com.zrdds.infrastructure.*;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;

import java.nio.charset.StandardCharsets;

import data_structure.*; // InferenceRequest/TaskList/WorkerResult/* + *TypeSupport/*Seq/*Reader/*Writer
import data_structure.Bytes;

/**
 * DispatcherPlusPrint：
 * - 保留原有功能：收到 InferenceRequest 先 ACK（写入 inference/aggregated）【如不需要可注释掉】
 * - 新增：将 InferenceRequest.input_blob 转发为 TaskList 到 inference/tasklist
 * - 新增：订阅 inference/worker_result，解析并在控制台打印每条 SingleResult
 *
 * 说明：按你的需求，当前不再发送 AggregatedResult 给客户端，只打印 Worker 返回的结果。
 */
public class Dispatcher {

    static final int DOMAIN_ID = 100;

    // Topics
    static final String TOPIC_REQUEST      = "inference/request";
    static final String TOPIC_TASKLIST     = "inference/tasklist";
    static final String TOPIC_WORKER_RES   = "inference/worker_result";
    static final String TOPIC_AGGREGATED   = "inference/aggregated"; // 仅用于 ACK，如不需要可移除

    // writers kept as fields for listeners
    private static TaskListDataWriter          taskWriter;
    private static AggregatedResultDataWriter  ackWriter;  // 仅用于 ACK，可选

    // ===== 收到 InferenceRequest：ACK + 转发 TaskList =====
    static final class ReqListener extends SimpleDataReaderListener<InferenceRequest, InferenceRequestSeq, InferenceRequestDataReader> {
        @Override
        public void on_process_sample(DataReader r, InferenceRequest req, SampleInfo info) {
            try {
                // --- 可选：ACK 回客户端（保留原有功能；若不需要，整段可注释掉）
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
                    System.out.println("[DispatcherPlusPrint] ACK -> " + rc + " req=" + ack.request_id);
                }

                // --- 转发给 Worker
                TaskList tl = new TaskList();
                tl.tasks    = req.input_blob; // 直接沿用请求内的任务列表
                tl.task_num = (tl.tasks != null) ? tl.tasks.length() : 0;
                tl.worker_id = "auto";
                tl.meta = new KVList(); tl.meta.value = new KVSeq();

                ReturnCode_t rc2 = taskWriter.write(tl, InstanceHandle_t.HANDLE_NIL_NATIVE);
                System.out.println("[DispatcherPlusPrint] →Worker TaskList n=" + tl.task_num + " rc=" + rc2 + " req=" + req.request_id);

            } catch (Throwable t) { t.printStackTrace(); }
        }
        @Override public void on_data_arrived(DataReader r, Object s, SampleInfo i) {}
    }

    // ===== 解析并打印 WorkerResult =====
    static final class WorkerResListener extends SimpleDataReaderListener<WorkerResult, WorkerResultSeq, WorkerResultDataReader> {
        @Override
        public void on_process_sample(DataReader r, WorkerResult wr, SampleInfo info) {
            try {
                int n = (wr.results != null) ? wr.results.length() : 0;
                System.out.println("[DispatcherPlusPrint] ←Worker got WorkerResult, count=" + n);
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
                    String preview = previewBytes(res.output_blob, 64);

                    System.out.println(String.format(
                            "  result[%d]: req=%s client=%s task=%s model=%s status=%s latency=%dms out_len=%d preview=%s",
                            i, reqId, cliId, taskId, model, status, latMs, outLen, preview));
                }
            } catch (Throwable t) { t.printStackTrace(); }
        }
        @Override public void on_data_arrived(DataReader r, Object s, SampleInfo i) {}
    }

    private static String previewBytes(Bytes b, int maxChars) {
        try {
            if (b == null || b.length() == 0) return "<empty>";
            byte[] arr = new byte[Math.min(b.length(), 1024)];
            b.to_array(arr, arr.length);
            String s = new String(arr, 0, Math.min(arr.length, maxChars), StandardCharsets.UTF_8);
            return s.replaceAll("\n", "\\n");
        } catch (Throwable e) {
            return "<decode-failed>";
        }
    }

    public static void main(String[] args) throws Exception {
        // 1) participant
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        DomainParticipant dp = dpf.create_participant(
                DOMAIN_ID,
                DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null, StatusKind.STATUS_MASK_NONE);
        if (dp == null) { System.err.println("create dp failed"); return; }

        // 2) register types
        InferenceRequestTypeSupport irTS = (InferenceRequestTypeSupport) InferenceRequestTypeSupport.get_instance();
        TaskListTypeSupport        tlTS = (TaskListTypeSupport)        TaskListTypeSupport.get_instance();
        WorkerResultTypeSupport    wrTS = (WorkerResultTypeSupport)    WorkerResultTypeSupport.get_instance();
        AggregatedResultTypeSupport agTS= (AggregatedResultTypeSupport) AggregatedResultTypeSupport.get_instance(); // 仅用于 ACK
        if (irTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                tlTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                wrTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                agTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed"); return;
        }

        // 3) topics
        Topic reqTopic = dp.create_topic(TOPIC_REQUEST,    irTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic tlTopic  = dp.create_topic(TOPIC_TASKLIST,   tlTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic wrTopic  = dp.create_topic(TOPIC_WORKER_RES, wrTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic ackTopic = dp.create_topic(TOPIC_AGGREGATED, agTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 4) pub/sub
        Publisher  pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 5) writers/readers
        taskWriter = (TaskListDataWriter)
                pub.create_datawriter(tlTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        ackWriter  = (AggregatedResultDataWriter)
                pub.create_datawriter(ackTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE); // 可选

        InferenceRequestDataReader reqReader = (InferenceRequestDataReader)
                sub.create_datareader(reqTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        WorkerResultDataReader wrReader = (WorkerResultDataReader)
                sub.create_datareader(wrTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 6) listeners
        reqReader.set_listener(new ReqListener(),        StatusKind.STATUS_MASK_ALL);
        wrReader.set_listener(new WorkerResListener(),   StatusKind.STATUS_MASK_ALL);

        System.out.println("DispatcherPlusPrint started. Press ENTER to exit.");
        System.in.read();

        // 7) cleanup
        dp.delete_contained_entities();
        dpf.delete_participant(dp);
        DomainParticipantFactory.finalize_instance();
    }
}
