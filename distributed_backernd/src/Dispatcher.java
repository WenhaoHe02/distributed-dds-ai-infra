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

import ai.*; // InferenceRequest/InferenceRequest* / AggregatedResult* / *TypeSupport / *DataReader/*DataWriter

/**
 * Minimal Dispatcher (client <-> dispatcher only):
 * - Subscribe: inference/request  (InferenceRequest)
 * - Publish  : inference/aggregated (AggregatedResult) — ACK only for now
 *
 * 说明：
 * 1) 仅处理客户端请求并返回一个接收确认（ACK），不做分发到 Worker。
 * 2) QoS 名称与示例 XML 对齐：reader 使用 "non_zerocopy_reliable"；writer 使用 "non_zerocopy_reliable"。
 */
public class Dispatcher {

    // ===== QoS / Topic / Domain =====
    private static final int    DOMAIN_ID           = 100;
    private static final String FACTORY_PROFILE     = "non_rio";                 // participantfactory_qos
    private static final String PARTICIPANT_PROFILE = "tcp_dp";                  // 或 "udp_dp" 视环境而定

    private static final String REQ_READER_QOS      = "non_zerocopy_reliable";   // DataReader QoS
    private static final String AGG_WRITER_QOS      = "non_zerocopy_reliable";   // DataWriter QoS（ACK 想保证必达）

    private static final String TOPIC_REQUEST       = "inference/request";
    private static final String TOPIC_AGGREGATED    = "inference/aggregated";    // 临时用于 ACK

    // ===== Listener：收到 InferenceRequest 就回 ACK =====
    private static final class RequestListener extends SimpleDataReaderListener<InferenceRequest, InferenceRequestSeq, InferenceRequestDataReader> {
        private final AggregatedResultDataWriter ackWriter;
        RequestListener(AggregatedResultDataWriter w) { this.ackWriter = w; }

        @Override
        public void on_process_sample(DataReader reader, InferenceRequest req, SampleInfo info) {
            try {
                AggregatedResult ack = new AggregatedResult();
                ack.request_id   = req.request_id;

                // 使用 ZRDDS sequence API：length() / get_at()
                if (req.input_blob != null && req.input_blob.length() > 0) {
                    SingleTask t0 = req.input_blob.get_at(0);
                    ack.client_id = (t0 != null && t0.client_id != null) ? t0.client_id : "";
                } else {
                    ack.client_id = "";
                }

                ack.status        = "RECEIVED";   // 或 "ACCEPTED"
                ack.error_message = "";
                ack.results       = new SingleResultSeq(); // ACK 阶段先空列表

                ReturnCode_t rc = ackWriter.write(ack, InstanceHandle_t.HANDLE_NIL_NATIVE);
                if (rc != ReturnCode_t.RETCODE_OK) {
                    System.err.println("[Dispatcher] ACK write failed: " + rc);
                } else {
                    System.out.println("[Dispatcher] ACK sent for request: " + ack.request_id);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        // SDK 抽象类要求的兜底方法：即使不用也要实现
        @Override
        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
            // 可选：如果 sample 类型匹配，转调 on_process_sample
            // if (sample instanceof InferenceRequest) {
            //     on_process_sample(reader, (InferenceRequest) sample, info);
            // }
        }
    }

    // ===== main =====
    public static void main(String[] args) throws Exception {
        // 1) init factory
        DomainParticipantFactory pf = DDSIF.init("ZRDDS_QOS_PROFILES.xml", FACTORY_PROFILE);
        if (pf == null) { System.err.println("DDSIF.init failed"); return; }

        // 2) create participant (profile)
        DomainParticipant dp = DDSIF.create_dp(DOMAIN_ID, PARTICIPANT_PROFILE);
        if (dp == null) { System.err.println("create_dp failed"); return; }

        InferenceRequestTypeSupport irTS =
                (InferenceRequestTypeSupport) InferenceRequestTypeSupport.get_instance();
        AggregatedResultTypeSupport agTS =
                (AggregatedResultTypeSupport) AggregatedResultTypeSupport.get_instance();
        if (irTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                || agTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed");
            return;
        }

        // 4) create topics —— 必须传 QoS/监听器/事件掩码
        Topic reqTopic = dp.create_topic(
                TOPIC_REQUEST,
                irTS.get_type_name(),                // 已注册的类型名
                DomainParticipant.TOPIC_QOS_DEFAULT, // TopicQos
                null,                                // TopicListener
                StatusKind.STATUS_MASK_NONE          // 事件掩码
        );

        Topic aggTopic = dp.create_topic(
                TOPIC_AGGREGATED,
                agTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE
        );
        if (reqTopic == null || aggTopic == null) {
            System.err.println("create topic failed");
            return;
        }
        // 5) pub/sub
        Publisher pub = dp.create_publisher(
                DomainParticipant.PUBLISHER_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE);
        Subscriber sub = dp.create_subscriber(
                DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE);

        // 6) writer/reader with QoS names from XML
        AggregatedResultDataWriter ackWriter = (AggregatedResultDataWriter)
                DDSIF.pub_topic(dp, TOPIC_AGGREGATED, agTS, AGG_WRITER_QOS, null);
        if (ackWriter == null) { System.err.println("pub aggregated topic failed"); return; }

        InferenceRequestDataReader reqReader = (InferenceRequestDataReader)
                DDSIF.sub_topic(dp, TOPIC_REQUEST, irTS, REQ_READER_QOS, new RequestListener(ackWriter));
        if (reqReader == null) { System.err.println("sub request topic failed"); return; }

        System.out.println("Dispatcher (client-only) started. Press ENTER to exit.");
        System.in.read();
        DDSIF.Finalize();
    }
}
