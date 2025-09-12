package com.example.ocrclient;

import android.util.Log;

import com.example.ocrclient.data_structure.*;
import com.example.ocrclient.data_structure.ResultUpdate;
import com.example.ocrclient.data_structure.ResultUpdateDataReader;
import com.example.ocrclient.data_structure.ResultUpdateSeq;
import com.example.ocrclient.data_structure.ResultUpdateTypeSupport;
import com.example.ocrclient.util.ResultSortUtil;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.domain.DomainParticipantFactoryQos;
import com.zrdds.domain.DomainParticipantQos;
import com.zrdds.infrastructure.InstanceStateKind;
import com.zrdds.infrastructure.LivelinessChangedStatus;
import com.zrdds.infrastructure.Property_t;
import com.zrdds.infrastructure.RequestedDeadlineMissedStatus;
import com.zrdds.infrastructure.RequestedIncompatibleQosStatus;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.SampleInfo;
import com.zrdds.infrastructure.SampleInfoSeq;
import com.zrdds.infrastructure.SampleLostStatus;
import com.zrdds.infrastructure.SampleRejectedStatus;
import com.zrdds.infrastructure.SampleStateKind;
import com.zrdds.infrastructure.StatusKind;
import com.zrdds.infrastructure.SubscriptionMatchedStatus;
import com.zrdds.infrastructure.ViewStateKind;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.DataReaderListener;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class DDSReceiveService {
    private static final String TAG = "DataReceiveByListener";
    private static final int DOMAIN_ID = 100;
    private static final String TOPIC_NAME = "inference/result_update";

    private DomainParticipant participant;
    private Subscriber subscriber;
    private Topic topic;
    private DataReader dataReader;

    public void work() {
        loadLibrary();

        try {
            // 1. 创建DomainParticipantFactory
            Log.i(TAG, "开始初始化ZRDDS...");
            DomainParticipantFactoryQos dpfQos = new DomainParticipantFactoryQos();
            dpfQos.dds_log.file_mask = 0;
            dpfQos.dds_log.console_mask = 0xffff;
            Property_t property = new Property_t();
            property.name = "sysctl.global.licence";
            property.value = "data:UserName: \nAuth Date: 2020/09/15:19:17:26\nExpire Date: 2025/10/21:19:17:26\nMACS:\nunlimited\nHDS:\nunlimited\nSignature:\nb4b93ac94879a73959465ad0692722934efb100a1069d1e91d4fc14596483cf651496531f7376f389b2a6cea9dc4b276f8cdd3ce171f2c333a5f6061e0033a94889282b1d142ca3709b69e6e88cd24252818bd543c1f66a1ae905bdb8b854e03055a1535fa262570fbefcdb7c05b63f872809cd57f82dfcc72cc495eee824ff0\nLastVerifyDate:2024/11/21:11:01:5022325d7c7825924f6f8c0ab42a65414c";
            dpfQos.property.value.ensure_length(0, 1);
            dpfQos.property.value.append(property);

            DomainParticipantFactory factory = DomainParticipantFactory.get_instance_w_qos(dpfQos);
            if (factory == null) {
                Log.e(TAG, "无法获取DomainParticipantFactory实例");
                return;
            }
            Log.i(TAG, "✓ DomainParticipantFactory创建成功");

            DomainParticipantQos dpQos = new DomainParticipantQos();
            factory.get_default_participant_qos(dpQos);
            dpQos.discovery_config.participant_liveliness_lease_duration.sec = 10;
            dpQos.discovery_config.participant_liveliness_assert_period.sec = 1;

            // 2. 创建域参与者
            participant = factory.create_participant(
                    DOMAIN_ID,
                    dpQos,
                    null, // listener
                    StatusKind.STATUS_MASK_NONE
            );

            if (participant == null) {
                Log.e(TAG, "创建DomainParticipant失败");
                return;
            }
            Log.i(TAG, "✓ DomainParticipant创建成功，Domain ID: " + DOMAIN_ID);

            // 3. 注册数据类型
            ResultUpdateTypeSupport typeSupport = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();
            ReturnCode_t rtn = typeSupport.register_type(participant, null);
            if (rtn != ReturnCode_t.RETCODE_OK) {
                Log.e(TAG, "注册数据类型失败");
                return;
            }
            Log.i(TAG, "✓ 数据类型注册成功");

            // 4. 创建Topic
            topic = participant.create_topic(
                    TOPIC_NAME,
                    typeSupport.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );
            if (topic == null) {
                Log.e(TAG, "创建Topic失败");
                return;
            }
            Log.i(TAG, "✓ Topic创建成功: " + TOPIC_NAME);

            // 5. 创建Subscriber
            createSubscriber();

        } catch (Exception e) {
            Log.e(TAG, "ZRDDS初始化失败", e);
        }
    }

    private void createSubscriber() {
        try {
            subscriber = participant.create_subscriber(
                    DomainParticipant.SUBSCRIBER_QOS_DEFAULT,
                    null,
                    StatusKind.STATUS_MASK_NONE
            );
            if (subscriber == null) {
                Log.e(TAG, "创建Subscriber失败");
                return;
            }

            DataReaderListener readerListener = new DataReaderListener() {
                @Override
                public void on_data_available(DataReader reader) {
                    Log.i(TAG, "📨 收到新数据！");
                    readData(dataReader);
                }

                @Override
                public void on_data_arrived(DataReader reader, Object obj, SampleInfo sampleInfo) {
                    Log.i(TAG, "📨 收到新数据！");
                    readData(dataReader);
                }

                @Override
                public void on_sample_lost(DataReader reader, SampleLostStatus status) {
                    Log.w(TAG, "数据丢失: " + status.total_count);
                }

                @Override
                public void on_sample_rejected(DataReader reader, SampleRejectedStatus status) {
                    Log.w(TAG, "数据被拒绝: " + status.total_count);
                }

                @Override
                public void on_requested_deadline_missed(DataReader reader, RequestedDeadlineMissedStatus status) {
                    Log.w(TAG, "请求截止时间错过: " + status.total_count);
                }

                @Override
                public void on_requested_incompatible_qos(DataReader reader, RequestedIncompatibleQosStatus status) {
                    Log.w(TAG, "请求的QoS不兼容: " + status.total_count);
                }

                @Override
                public void on_liveliness_changed(DataReader reader, LivelinessChangedStatus status) {
                    Log.i(TAG, "存活状态改变: alive=" + status.alive_count + ", not_alive=" + status.not_alive_count);
                }

                @Override
                public void on_subscription_matched(DataReader reader, SubscriptionMatchedStatus status) {
                    Log.i(TAG, "订阅匹配: current=" + status.current_count + ", total=" + status.total_count);
                }
            };

            dataReader = subscriber.create_datareader(
                    topic,
                    Subscriber.DATAREADER_QOS_DEFAULT,
                    readerListener,
                    StatusKind.STATUS_MASK_ALL
            );
            if (dataReader == null) {
                Log.e(TAG, "创建DataReader失败");
                return;
            }
            Log.i(TAG, "✓ Subscriber和DataReader创建成功");

        } catch (Exception e) {
            Log.e(TAG, "创建Subscriber失败", e);
        }
    }

    // 处理接收到的数据
    public void readData(DataReader reader) {
        Log.i(TAG,"开始处理数据");
        try {
            ResultUpdateDataReader dr = (ResultUpdateDataReader) reader;
            ResultUpdateSeq dataSeq = new ResultUpdateSeq();
            SampleInfoSeq infoSeq = new SampleInfoSeq();

            ReturnCode_t rtn = dr.take(
                    dataSeq, infoSeq, -1,
                    SampleStateKind.ANY_SAMPLE_STATE,
                    ViewStateKind.ANY_VIEW_STATE,
                    InstanceStateKind.ANY_INSTANCE_STATE
            );

            if (rtn == ReturnCode_t.RETCODE_OK) {
                for (int i = 0; i < infoSeq.length(); i++) {
                    if (!infoSeq.get_at(i).valid_data) continue;

                    ResultUpdate result = dataSeq.get_at(i);

                    // 直接传递给ResultDataManager处理，验证逻辑已在其中实现
                    ResultDataManager.getInstance().handleResultUpdate(result);
                }
                dr.return_loan(dataSeq, infoSeq);
            }
        } catch (Exception e) {
            Log.e(TAG, "读取数据时异常", e);
        }
    }

    // 加载DDS库
    private static boolean hasLoad = false;
    private static void loadLibrary() {
        if (!hasLoad) {
            System.loadLibrary("ZRDDS_JAVA");
            hasLoad = true;
        }
    }
}