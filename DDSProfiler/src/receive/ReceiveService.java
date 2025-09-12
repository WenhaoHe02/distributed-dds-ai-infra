package receive;

import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.*;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.DataReaderListener;
import com.zrdds.subscription.DataReaderQos;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;
import data_structure.*;

public class ReceiveService {
    private static final int DOMAIN_ID = 100;
    private static final String TOPIC_RESULT_UPDATE = "inference/result_update";
    private static final String LOG_TAG = "[receive.ReceiveService] ";

    private DomainParticipant dp;
    private ResultUpdateTypeSupport resultUpdateTypeSupport;
    private Topic tp;
    private Subscriber sub;
    private DataReader dr;
    private ListenerDataReaderListener listener;


    public void initDDS() {
        loadLibrary();
        ReturnCode_t rtn;

        // 创建域参与者
        dp = DomainParticipantFactory.get_instance().create_participant(
                DOMAIN_ID, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        if (dp == null) {
            System.out.println("create dp failed");
            return;
        }

        // 注册数据类型
        resultUpdateTypeSupport = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();
        rtn = resultUpdateTypeSupport.register_type(dp, null);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("register type failed");
            return;
        }

        // 创建主题
        tp = dp.create_topic(TOPIC_RESULT_UPDATE, resultUpdateTypeSupport.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (tp == null) {
            System.out.println("create tp failed");
            return;
        }

        // 创建订阅者
        sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        if (sub == null) {
            System.out.println("create sub failed");
            return;
        }

        // 创建监听器
        listener = new ListenerDataReaderListener();

//        //TODO: QOS策略配置，保证不会漏消息等等
        DataReaderQos qos = new DataReaderQos();

        qos.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        qos.history.kind = HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS;
        qos.history.depth=100;
//        qos.resource_limits.max_samples = 10000;
//        qos.resource_limits.max_instances = 1000;
//        qos.resource_limits.max_samples_per_instance = 1000;
//        qos.deadline.period.sec = 1;
//        qos.liveliness.kind = LivelinessQosPolicyKind.AUTOMATIC_LIVELINESS_QOS;

        // 创建数据读者
        dr = sub.create_datareader(tp, qos, listener,
                StatusKind.DATA_AVAILABLE_STATUS);
        if (dr == null) {
            System.out.println("create dr failed");
        }
    }

    public void clean() {
        ReturnCode_t rtn;
        // 释放DDS资源
        rtn = dp.delete_contained_entities();
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("dp delete contained entities failed");
            return;
        }

        rtn = DomainParticipantFactory.get_instance().delete_participant(dp);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("dpf delete dp failed");
            return;
        }
        DomainParticipantFactory.finalize_instance();

    }

    // 已加载库标识
    private static boolean hasLoad = false;

    // 加载DDS库
    public static void loadLibrary() {
        if (!hasLoad) {
            System.loadLibrary("ZRDDS_JAVA");
            hasLoad = true;
        }
    }
}

class ListenerDataReaderListener implements DataReaderListener {
    private static final String LOG_TAG = "[ReceiveServiceListener] ";
    private final DataAnalyse dataAnalyse;

    public ListenerDataReaderListener() {
        dataAnalyse = new DataAnalyse();
    }

    public void on_data_available(DataReader dataReader) {
        // 记录当前时间
        // TODO: 获取时间的时机存疑
        long receiveTime = System.currentTimeMillis();

        //System.out.println("Thread: " + Thread.currentThread().getName());
        System.out.println(LOG_TAG + "接收到新数据");
        ResultUpdateDataReader dr = (ResultUpdateDataReader) (dataReader);
        ResultUpdateSeq dataSeq = new ResultUpdateSeq();
        SampleInfoSeq infoSeq = new SampleInfoSeq();
        ReturnCode_t rtn;

        // 取出接收数据
        rtn = dr.take(dataSeq, infoSeq, -1, SampleStateKind.ANY_SAMPLE_STATE,
                ViewStateKind.ANY_VIEW_STATE, InstanceStateKind.ANY_INSTANCE_STATE);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("take data failed");
        }

        // 遍历读取接收数据
        for (int i = 0; i < infoSeq.length(); ++i) {
            // 在使用数据之前，应检查数据的有效性
            if (!infoSeq.get_at(i).valid_data) {
                continue;
            }
            // 打印接收数据
            ResultUpdateTypeSupport ResultUpdate = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();
            System.out.println(LOG_TAG + "接收数据：");
            ResultUpdate ru = dataSeq.get_at(i);
            System.out.println((LOG_TAG + "request_id: " + ru.request_id + " client_id: " + ru.client_id));
            //ResultUpdate.print_sample(dataSeq.get_at(i));

            // 交给DataAnalyse处理
            dataAnalyse.processResultUpdate(ru, receiveTime);
        }

        // 返还数据空间
        rtn = dr.return_loan(dataSeq, infoSeq);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("return loan failed");
        }
    }

    public void on_liveliness_changed(DataReader arg0, LivelinessChangedStatus arg1) {
        // TODO 自动生成的方法存根
    }

    public void on_requested_deadline_missed(DataReader arg0, RequestedDeadlineMissedStatus arg1) {
        // TODO 自动生成的方法存根
    }

    public void on_requested_incompatible_qos(DataReader arg0, RequestedIncompatibleQosStatus arg1) {
        // TODO 自动生成的方法存根
    }

    public void on_sample_lost(DataReader arg0, SampleLostStatus arg1) {
        // TODO 自动生成的方法存根
    }

    public void on_sample_rejected(DataReader arg0, SampleRejectedStatus arg1) {
        // TODO 自动生成的方法存根
    }

    public void on_subscription_matched(DataReader arg0, SubscriptionMatchedStatus arg1) {
        // TODO 自动生成的方法存根
    }

    public void on_data_arrived(DataReader arg0, Object arg1, SampleInfo arg2) {
        // TODO 自动生成的方法存根
    }
}

