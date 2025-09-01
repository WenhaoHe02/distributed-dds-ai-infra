import com.zrdds.infrastructure.*;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.*;

import  ai_train.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controller：控制端调度训练、收集客户端参数、聚合模型、评估
 */
public class Controller {

    private static final int DOMAIN_ID = 200;
    private static final String FACTORY_PROFILE     = "non_rio";
    private static final String PARTICIPANT_PROFILE = "tcp_dp";

    private static final String CLIENT_UPDATE_READER_QOS = "non_zerocopy_reliable";
    private static final String TRAIN_CMD_WRITER_QOS     = "non_zerocopy_reliable";
    private static final String MODEL_BLOB_WRITER_QOS    = "non_zerocopy_reliable";

    private static final String TOPIC_TRAIN_CMD     = "train/train_cmd";
    private static final String TOPIC_CLIENT_UPDATE = "train/client_update";
    private static final String TOPIC_MODEL_BLOB    = "train/model_blob";

    // 存放每轮收到的客户端参数
    private static final ConcurrentMap<Integer, List<ai_train.ClientUpdate>> updatesMap = new ConcurrentHashMap<>();

    private DomainParticipant dp;
    private Publisher publisher;
    private Subscriber subscriber;

    private DataWriter trainCmdWriter;
    private DataWriter modelBlobWriter;
    private DataReader clientUpdateReader;

    private AtomicLong roundCounter = new AtomicLong(1);

    private static class ClientUpdateListener extends SimpleDataReaderListener<ai_train.ClientUpdate, ai_train.ClientUpdateSeq, ai_train.ClientUpdateDataReader> {
        @Override
        public void on_process_sample(DataReader reader, ai_train.ClientUpdate cu, SampleInfo info) {
            if (cu == null) return;
            updatesMap.computeIfAbsent(cu.round_id, k -> new ArrayList<>()).add(cu);
            System.out.println("[Controller] Received ClientUpdate from client " + cu.client_id + " round=" + cu.round_id);
        }

        @Override
        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
        }

    }

    public static void main(String[] args) throws Exception {
        Controller ctrl = new Controller();
        ctrl.init();

        ctrl.runTrainingRound(5000, 5, 0.01, 12345); // 示例：subset=5000, epochs=5, lr=0.01, seed=12345
        DDSIF.Finalize();
    }

    private void init() {

        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        if (dpf == null) throw new RuntimeException("DDSIF.init failed");
        dp = dpf.create_participant(DOMAIN_ID, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null, StatusKind.STATUS_MASK_NONE);
        if (dp == null) throw new RuntimeException("create participant failed");

        // 注册类型

        ai_train.TrainCmdTypeSupport trainCmdTS=(ai_train.TrainCmdTypeSupport) ai_train.TrainCmdTypeSupport.get_instance();
        ai_train.ClientUpdateTypeSupport clientUpdateTS=(ai_train.ClientUpdateTypeSupport) ai_train.ClientUpdateTypeSupport.get_instance();
        ai_train.ModelBlobTypeSupport modelBlobTS=(ai_train.ModelBlobTypeSupport) ai_train.ModelBlobTypeSupport.get_instance();
        if (trainCmdTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                || clientUpdateTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                || modelBlobTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed");
            return;
        }

        // 创建 Topic
        Topic trainCmdTopic = dp.create_topic(TOPIC_TRAIN_CMD,
                ai_train.TrainCmdTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        Topic clientUpdateTopic = dp.create_topic(TOPIC_CLIENT_UPDATE,
                ai_train.ClientUpdateTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        Topic modelBlobTopic = dp.create_topic(TOPIC_MODEL_BLOB,
                ai_train.ModelBlobTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        publisher = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        subscriber = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        trainCmdWriter = publisher.create_datawriter(trainCmdTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        modelBlobWriter = publisher.create_datawriter(modelBlobTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        clientUpdateReader = subscriber.create_datareader(clientUpdateTopic, Subscriber.DATAREADER_QOS_DEFAULT,
                new ClientUpdateListener(), StatusKind.DATA_AVAILABLE_STATUS);
    }

    // 发起一轮训练
    public void runTrainingRound(long subsetSize, int epochs, double lr, int seed) throws InterruptedException {
        int roundId = (int)roundCounter.getAndIncrement();
        System.out.println("[Controller] Starting round " + roundId);
        long tStart = System.currentTimeMillis();
        // 1) 发送 TrainCmd
        ai_train.TrainCmd cmd = new ai_train.TrainCmd();
        cmd.round_id = roundId;
        cmd.subset_size = (int) subsetSize;
        cmd.epochs = epochs;
        cmd.lr = lr;
        cmd.seed = seed;

        ReturnCode_t rc = ((ai_train.TrainCmdDataWriter)trainCmdWriter).write(cmd, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {


            System.err.println("[Controller] Failed to write TrainCmd: " + rc);
        }

        // 2) 等待客户端返回 ClientUpdate
        List<ai_train.ClientUpdate> collected = waitForClientUpdates(roundId, 3, 60_00000); // 假设3个客户端，60s超时
        if (collected.isEmpty()) {
            System.err.println("[Controller] No client updates received for round " + roundId);
            return;
        }

        System.out.println("[Controller] Collected " + collected.size() + " client updates.");

        // 3) 参数聚合
        byte[] aggregated = aggregateParameters(collected);

        // 4) 发布聚合模型
        ai_train.ModelBlob blob = new ai_train.ModelBlob();
        blob.round_id = roundId;
        blob.data = new ai_train.Bytes();
        blob.data.ensure_length(aggregated.length, aggregated.length);
        for (int i = 0; i < aggregated.length; i++) blob.data.set_at(i, aggregated[i]);

        rc = ((ai_train.ModelBlobDataWriter)modelBlobWriter).write(blob, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller] Failed to write ModelBlob: " + rc);
        } else {
            System.out.println("[Controller] Published aggregated model for round " + roundId);
        }
        long tEnd = System.currentTimeMillis();    // ✅ 计时结束
        long duration = tEnd - tStart;
        System.out.println("[e2e time] " + duration);

        // 5) 测试评估
        evaluateModel(aggregated);
    }

    private List<ai_train.ClientUpdate> waitForClientUpdates(int roundId, int expectedClients, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        List<ai_train.ClientUpdate> resultList = new ArrayList<>();
        while (System.currentTimeMillis() - start < timeoutMs && resultList.size() < expectedClients) {
            List<ai_train.ClientUpdate> list = updatesMap.get(roundId);
            if (list != null) {
                resultList.addAll(list);
            }
            Thread.sleep(100); // 每100ms检查一次
        }
        return resultList;
    }

    // 简单参数聚合（逐字节平均）
    private byte[] aggregateParameters(List<ai_train.ClientUpdate> updates) {
        if (updates.isEmpty()) return new byte[0];
        int len = updates.get(0).data.length();
        byte[] result = new byte[len];
        for (ai_train.ClientUpdate cu : updates) {
            for (int i = 0; i < len; i++) {
                result[i] += cu.data.get_at(i) / updates.size();
            }
        }
        return result;
    }

    private void evaluateModel(byte[] modelData) {
        System.out.println("[Controller] Evaluating model, size=" + modelData.length);
        // TODO: 测试集推理，记录时间和识别率
    }

}