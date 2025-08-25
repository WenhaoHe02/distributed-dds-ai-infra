import com.zrdds.infrastructure.*;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;

import ai.*; // 生成的类型：InferenceRequest*, AggregatedResult*, SingleTask*, *TypeSupport 等

public class Dispatcher {

    static final int DOMAIN_ID = 100; // 和客户端一致

    // 收到 request 就回 ACK
    static final class ReqListener extends SimpleDataReaderListener<InferenceRequest, InferenceRequestSeq, InferenceRequestDataReader> {
        private final AggregatedResultDataWriter ackWriter;
        ReqListener(AggregatedResultDataWriter w){ this.ackWriter = w; }

        @Override
        public void on_process_sample(DataReader r, InferenceRequest req, SampleInfo info) {
            try {
                AggregatedResult ack = new AggregatedResult();
                ack.request_id = req.request_id;

                if (req.input_blob != null && req.input_blob.length() > 0) {
                    SingleTask t0 = req.input_blob.get_at(0);
                    ack.client_id = (t0 != null && t0.client_id != null) ? t0.client_id : "";
                } else {
                    ack.client_id = "";
                }
                ack.status        = "RECEIVED";
                ack.error_message = "";
                ack.results       = new SingleResultSeq(); // 先空

                ReturnCode_t rc = ackWriter.write(ack, InstanceHandle_t.HANDLE_NIL_NATIVE);
                System.out.println("[Dispatcher] ACK " + (rc==ReturnCode_t.RETCODE_OK?"OK ":"FAIL ") + ack.request_id);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        // 抽象类要求的兜底；可留空
        @Override public void on_data_arrived(DataReader r, Object s, SampleInfo i) {}
    }

    public static void main(String[] args) throws Exception {
        // 1) 工厂/参与者（全默认 QoS）
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        DomainParticipant dp = dpf.create_participant(
                DOMAIN_ID,
                DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE);
        if (dp == null) { System.err.println("create dp failed"); return; }

        // 2) 注册类型（注意返回码）
        InferenceRequestTypeSupport irTS = (InferenceRequestTypeSupport) InferenceRequestTypeSupport.get_instance();
        AggregatedResultTypeSupport agTS = (AggregatedResultTypeSupport) AggregatedResultTypeSupport.get_instance();
        if (irTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                agTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed"); return;
        }

        // 3) Topic（默认 QoS）
        Topic reqTopic = dp.create_topic("inference/request", irTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic aggTopic = dp.create_topic("inference/aggregated", agTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (reqTopic==null || aggTopic==null) { System.err.println("create topic failed"); return; }

        // 4) Pub/Sub（默认 QoS）
        Publisher  pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 5) Writer/Reader（默认 QoS）
        AggregatedResultDataWriter ackWriter = (AggregatedResultDataWriter)
                pub.create_datawriter(aggTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        InferenceRequestDataReader reqReader = (InferenceRequestDataReader)
                sub.create_datareader(reqTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 6) 绑定监听器
        reqReader.set_listener(new ReqListener(ackWriter), StatusKind.STATUS_MASK_ALL);

        System.out.println("Dispatcher started. Press ENTER to exit.");
        System.in.read();

        // 7) 清理
        dp.delete_contained_entities();
        dpf.delete_participant(dp);
        DomainParticipantFactory.finalize_instance();
    }
}
