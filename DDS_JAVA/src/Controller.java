import com.zrdds.infrastructure.*;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.*;

import  ai_train.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import java.io.FileWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        List<ai_train.ClientUpdate> collected = waitForClientUpdates(roundId, 2, 100_000); // 假设3个客户端，60s超时
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

    // 调用 Python 脚本 aggregator.py 进行参数聚合
    private byte[] aggregateParameters(List<ai_train.ClientUpdate> updates) {
        try {
            // 1) 构建 JSON，传 Base64 编码后的 bytes
            StringBuilder sbJson = new StringBuilder();
            sbJson.append("[");
            for (int idx = 0; idx < updates.size(); idx++) {
                ai_train.ClientUpdate cu = updates.get(idx);
                sbJson.append("{");
                sbJson.append("\"client_id\":").append(cu.client_id).append(",");
                sbJson.append("\"round_id\":").append(cu.round_id).append(",");
                sbJson.append("\"num_samples\":").append(cu.num_samples).append(",");

                String b64 = Base64.getEncoder().encodeToString(cu.data.get_contiguous_buffer());
                sbJson.append("\"weights_b64\":\"").append(b64).append("\"");

                sbJson.append("}");
                if (idx != updates.size() - 1) sbJson.append(",");
            }
            sbJson.append("]");

            // 2) 调用 Python 脚本
            ProcessBuilder pb = new ProcessBuilder(
                    "python",
                    "D:/Study/SummerSchool/codes/distributed-dds-ai-serving-system/distributed_training/train/aggregator.py"
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            // 保存到 txt 文件
            String pythonInput = sbJson.toString();

            /*
            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter("python_input.txt"))) {
                fileWriter.write(pythonInput);
            }
            */

            // 继续写入 Python 进程
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
                writer.write(pythonInput);
                //System.out.println(pythonInput);
                writer.flush();
            }

            // 3) 读取 Python 输出
            StringBuilder outBuf = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    outBuf.append(line);
                }
            }

            p.waitFor();

            String resultJson = outBuf.toString();
            //System.out.println("result: " + resultJson);

            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter("resultJson.txt"))) {
                fileWriter.write(resultJson);
            }



            // 4) 用 JSON 解析
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(resultJson);
            String aggB64 = root.get("weights_b64").asText();

            return Base64.getDecoder().decode(aggB64);

//            // 4) 解析 Python 返回的 JSON，提取 Base64 编码的聚合权重
//            int start = resultJson.indexOf("\"weights_b64\":\"") + 15;
//            int end = resultJson.indexOf("\"", start);
//
//            System.out.println("start: " + start + ", end: " + end);
//            String aggB64 = resultJson.substring(start, end);
//
//            try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter("aggB64.txt"))) {
//                fileWriter.write(aggB64);
//            }
//
//            // Base64 -> byte[]
//            return Base64.getDecoder().decode(aggB64);

        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0];
        }
    }


    private void evaluateModel(byte[] modelData) {
        System.out.println("[Controller] Evaluating model, size=" + modelData.length);
        // TODO: 测试集推理，记录时间和识别率
    }

}
