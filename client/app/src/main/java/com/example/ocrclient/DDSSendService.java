package com.example.ocrclient;

import android.util.Log;

import com.example.ocrclient.data_structure.*;
import com.example.ocrclient.data_structure.InferenceRequest;
import com.example.ocrclient.data_structure.InferenceRequestDataWriter;
import com.example.ocrclient.data_structure.InferenceRequestTypeSupport;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.domain.DomainParticipantFactoryQos;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.Property_t;
import com.zrdds.infrastructure.PublicationMatchedStatus;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.StatusKind;
import com.zrdds.publication.Publisher;
import com.zrdds.topic.Topic;
import com.zrdds.topic.TypeSupport;

public class DDSSendService {
    private static final String TAG = "DDSSendService";
    private static final Integer DOMAIN_ID = 100;
    private static final String TOPIC_NAME = "inference/request";
    private DomainParticipant participant;
    private Publisher publisher;
    private InferenceRequestDataWriter inferenceRequestWriter;
    private Topic topic;

    /**
     * 初始化DDS组件
     *
     * @return 是否初始化成功
     */
    public boolean initializeDDS() {
        try {
            Log.d(TAG, "开始初始化DDS组件");
            DomainParticipantFactoryQos dpfQos = new DomainParticipantFactoryQos();
            dpfQos.dds_log.file_mask = 0;
            dpfQos.dds_log.console_mask = 0xffff;
            Property_t property = new Property_t();
            property.name = "sysctl.global.licence";
            property.value = "data:UserName: \nAuth Date: 2020/09/15:19:17:26\nExpire Date: 2025/10/21:19:17:26\nMACS:\nunlimited\nHDS:\nunlimited\nSignature:\nb4b93ac94879a73959465ad0692722934efb100a1069d1e91d4fc14596483cf651496531f7376f389b2a6cea9dc4b276f8cdd3ce171f2c333a5f6061e0033a94889282b1d142ca3709b69e6e88cd24252818bd543c1f66a1ae905bdb8b854e03055a1535fa262570fbefcdb7c05b63f872809cd57f82dfcc72cc495eee824ff0\nLastVerifyDate:2024/11/21:11:01:5022325d7c7825924f6f8c0ab42a65414c";
            dpfQos.property.value.ensure_length(0, 1);
            dpfQos.property.value.append(property);

            DomainParticipantFactory factory = DomainParticipantFactory.get_instance_w_qos(dpfQos);

            
            // 创建DomainParticipant
            participant = factory
                    .create_participant(
                            DOMAIN_ID,
                            DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                            null,
                            StatusKind.STATUS_MASK_NONE
                    );

            if (participant == null) {
                Log.e(TAG, "创建DomainParticipant失败");
                return false;
            }
            Log.d(TAG, "DomainParticipant创建成功");

            // 注册数据类型
            TypeSupport typeSupport = InferenceRequestTypeSupport.get_instance();
            ReturnCode_t registerResult = typeSupport.register_type(participant, null);
            if (!registerResult.equals(ReturnCode_t.RETCODE_OK)) {
                Log.e(TAG, "注册类型失败，错误码: " + registerResult);
                return false;
            }
            Log.d(TAG, "类型注册成功");

            // 创建Topic
            topic = participant.create_topic(
                    TOPIC_NAME,
                    typeSupport.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );

            if (topic == null) {
                Log.e(TAG, "创建Topic失败");
                return false;
            }
            Log.d(TAG, "Topic创建成功");

            // 创建Publisher
            publisher = participant.create_publisher(
                    DomainParticipant.PUBLISHER_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );

            if (publisher == null) {
                Log.e(TAG, "创建Publisher失败");
                return false;
            }
            Log.d(TAG, "Publisher创建成功");

            // 创建DataWriter
            inferenceRequestWriter = (InferenceRequestDataWriter) publisher.create_datawriter(
                    topic,
                    Publisher.DATAWRITER_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );

            if (inferenceRequestWriter == null) {
                Log.e(TAG, "创建DataWriter失败");
                return false;
            }
            Log.d(TAG, "DataWriter创建成功");

            Log.d(TAG, "DDS组件初始化完成");
            return true;
        } catch (Exception e) {
            Log.e(TAG, "初始化DDS组件时发生异常", e);
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 发送InferenceRequest数据
     *
     * @param inferenceRequest 要发送的请求数据
     * @return 是否发送成功
     */
    public boolean sendInferenceRequest(InferenceRequest inferenceRequest) {
        try {

            Log.d(TAG, "开始发送InferenceRequest数据");
            
            if (inferenceRequestWriter == null) {
                Log.e(TAG, "DataWriter未初始化");
                return false;
            }
//
//            // 创建 writer 成功后：
//            PublicationMatchedStatus st = new PublicationMatchedStatus();
//
//            new Thread(() -> {
//                try {
//                    while (true) {
//                        ReturnCode_t rc = inferenceRequestWriter.get_publication_matched_status(st);
//                        if (rc != ReturnCode_t.RETCODE_OK) {
//                            Log.e(TAG, "get_publication_matched_status rc=" + rc);
//                            break;
//                        }
//                        Log.d(TAG, "PUB matched: current=" + st.current_count +
//                                " total=" + st.total_count +
//                                " Δ=" + st.current_count_change +
//                                " lastSub=" + (st.last_subscription_handle != null ?
//                                st.last_subscription_handle.value : 0));
//
//                        if (st.current_count > 0) {
//                            Log.d(TAG, "已匹配到 Reader，可以安全发送数据");
//                            break;
//                        }
//                        Thread.sleep(200); // 200ms 间隔轮询
//                    }
//                } catch (InterruptedException ignored) {}
//            }).start();

            if (inferenceRequest == null) {
                Log.e(TAG, "InferenceRequest为空");
                return false;
            }

            Log.d(TAG, "请求ID: " + inferenceRequest.request_id);
            Log.d(TAG, "任务数量: " + inferenceRequest.tasks.length());

            ReturnCode_t result = inferenceRequestWriter.write(inferenceRequest, InstanceHandle_t.HANDLE_NIL_NATIVE);
            boolean success = result.equals(ReturnCode_t.RETCODE_OK);
            
            if (success) {
                Log.d(TAG, "数据发送成功");
            } else {
                Log.e(TAG, "数据发送失败，错误码: " + result);
            }
            
            return success;
        } catch (Exception e) {
            Log.e(TAG, "发送InferenceRequest数据时发生异常", e);
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 释放DDS资源
     */
    public void releaseDDS() {
        try {
            Log.d(TAG, "开始释放DDS资源");
            
            if (inferenceRequestWriter != null && publisher != null) {
                publisher.delete_datawriter(inferenceRequestWriter);
                inferenceRequestWriter = null;
                Log.d(TAG, "DataWriter已释放");
            }

            if (publisher != null && participant != null) {
                participant.delete_publisher(publisher);
                publisher = null;
                Log.d(TAG, "Publisher已释放");
            }

            if (topic != null && participant != null) {
                participant.delete_topic(topic);
                topic = null;
                Log.d(TAG, "Topic已释放");
            }

            if (participant != null) {
                DomainParticipantFactory.get_instance().delete_participant(participant);
                participant = null;
                Log.d(TAG, "DomainParticipant已释放");
            }
            
            Log.d(TAG, "DDS资源释放完成");
        } catch (Exception e) {
            Log.e(TAG, "释放DDS资源时发生异常", e);
            e.printStackTrace();
        }
    }
}