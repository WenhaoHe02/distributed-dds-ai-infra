package patcher;


import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.PublicationMatchedStatus;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.StatusKind;
import com.zrdds.publication.Publisher;
import com.zrdds.topic.Topic;

import data_structure.*; // InferenceRequest / InferenceRequestDataWriter / InferenceRequestTypeSupport

/**
 * 简单发送器：往 "inference/request" 发一条 InferenceRequest（tasks 可为空）
 */
public class TestSender {

    private static final int DOMAIN_ID = 100;
    private static final String TOPIC_INFER_REQ = "inference/request";

    public static void main(String[] args) throws Exception {
        DomainParticipant dp = null;
        Publisher pub = null;
        Topic reqTopic = null;
        InferenceRequestDataWriter writer = null;

        try {
            // 1) Participant
            DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
            if (dpf == null) {
                System.err.println("DomainParticipantFactory.get_instance() == null");
                return;
            }
            dp = dpf.create_participant(
                    DOMAIN_ID,
                    DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE);
            if (dp == null) { System.err.println("create_participant failed"); return; }

            // 2) 注册类型
            ReturnCode_t rc = InferenceRequestTypeSupport.get_instance().register_type(dp, null);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("register_type failed: " + rc);
                return;
            }

            // 3) Topic
            reqTopic = dp.create_topic(
                    TOPIC_INFER_REQ,
                    InferenceRequestTypeSupport.get_instance().get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE);
            if (reqTopic == null) { System.err.println("create_topic failed"); return; }

            // 4) Publisher & DataWriter（用默认 QoS；若需可靠，改用 DataWriterQos 显式设置）
            pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (pub == null) { System.err.println("create_publisher failed"); return; }
            writer = (InferenceRequestDataWriter) pub.create_datawriter(
                    reqTopic,
                    Publisher.DATAWRITER_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE);
            if (writer == null) { System.err.println("create_datawriter failed"); return; }

            // 5) 构造一条可空的 InferenceRequest（tasks 为空即可）
            InferenceRequest req = new InferenceRequest();
            req.request_id = "req-" + System.currentTimeMillis();
            req.tasks = new SingleTaskSeq(); // 空任务列表

            // 如果你想发一个最小的占位任务，取消以下注释：
//            SingleTask t = new SingleTask();
//            t.request_id = req.request_id;
//            t.task_id = "t-1";
//            t.model_id = "model-ocr";
//            t.client_id = "client-test";
//            t.payload = new Bytes(); // 空负载
//            t.payload.from_array(new byte[0], 0);
//            SingleTaskSeq seq = new SingleTaskSeq();
//            seq.ensure_length(1, 1);
//            seq.set_at(0, t);
//            req.tasks = seq;
//            PublicationMatchedStatus stt = new PublicationMatchedStatus();
//            try {
//                while (true) {
//                    ReturnCode_t rccc = writer.get_publication_matched_status(stt);
//                    if (rccc != ReturnCode_t.RETCODE_OK) {
//                        System.out.println("get_publication_matched_status rc=" + rc);
//                        break;
//                    }
//                    System.out.println("PUB matched: current=" + stt.current_count +
//                            " total=" + stt.total_count +
//                            " Δ=" + stt.current_count_change +
//                            " lastSub=" + (stt.last_subscription_handle != null ?
//                            stt.last_subscription_handle.value : 0));
//
//                    if (stt.current_count > 0) {
//                        System.out.println("已匹配到 Reader，可以安全发送数据");
//                        break;
//                    }
//                    Thread.sleep(200); // 200ms 间隔轮询
//                }
//            } catch (InterruptedException ignored) {}
            // 6) 发送
            ReturnCode_t wrc = writer.write(req, InstanceHandle_t.HANDLE_NIL_NATIVE /* 部分实现用 HANDLE_NIL */);
//            System.out.println("wrc" + wrc);
//            PublicationMatchedStatus st = new PublicationMatchedStatus();
//                try {
//                    while (true) {
//                        ReturnCode_t rcc = writer.get_publication_matched_status(st);
//                        if (rcc != ReturnCode_t.RETCODE_OK) {
//                            System.out.println("get_publication_matched_status rc=" + rc);
//                            break;
//                        }
//                        System.out.println("PUB matched: current=" + st.current_count +
//                                " total=" + st.total_count +
//                                " Δ=" + st.current_count_change +
//                                " lastSub=" + (st.last_subscription_handle != null ?
//                                st.last_subscription_handle.value : 0));
//
//                        if (st.current_count > 0) {
//                            System.out.println("已匹配到 Reader，可以安全发送数据");
//                            break;
//                        }
//                        Thread.sleep(200); // 200ms 间隔轮询
//                    }
//                } catch (InterruptedException ignored) {}

            System.out.println("write() rc = " + wrc + ", request_id=" + req.request_id +
                    ", tasks_len=" + req.tasks.length());

            System.out.println("Sent. Press ENTER to exit...");
            System.in.read();

        } finally {
            try { if (dp != null) DomainParticipantFactory.get_instance().delete_participant(dp); } catch (Throwable ignored) {}
            System.out.println("Sender stopped.");
        }
    }
}

