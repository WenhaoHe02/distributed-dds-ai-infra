import ai_train.Bytes;
import com.zrdds.infrastructure.*;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.*;

import ai_train.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controller：控制端调度训练、收集客户端参数、聚合模型、评估
 */
public class Controller_v1 {

    private static final int DOMAIN_ID = 200;

    private static final String TOPIC_TRAIN_CMD     = "train/train_cmd";
    private static final String TOPIC_CLIENT_UPDATE = "train/client_update";
    private static final String TOPIC_MODEL_BLOB    = "train/model_blob";

    // <<< 新增：期望客户端数量 / 超时 >>>
    private static final int EXPECTED_CLIENTS = 2;
    private static final long TIMEOUT_MS      = 60_00000;

    // 每轮收到的客户端更新
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
            if (cu == null || info == null || !info.valid_data) return;
            updatesMap.computeIfAbsent(cu.round_id, k -> Collections.synchronizedList(new ArrayList<>())).add(cu);
            System.out.println("[Controller] Received ClientUpdate from client " + cu.client_id + " round=" + cu.round_id +
                    " bytes=" + (cu.data == null ? 0 : cu.data.length()) + " nsamples=" + cu.num_samples);
        }
        @Override public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {}
    }

    public static void main(String[] args) throws Exception {
        Controller_v1 ctrl = new Controller_v1();
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
        TrainCmdTypeSupport     trainCmdTS     = (TrainCmdTypeSupport)     TrainCmdTypeSupport.get_instance();
        ClientUpdateTypeSupport clientUpdateTS = (ClientUpdateTypeSupport) ClientUpdateTypeSupport.get_instance();
        ModelBlobTypeSupport    modelBlobTS    = (ModelBlobTypeSupport)    ModelBlobTypeSupport.get_instance();
        if (trainCmdTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                || clientUpdateTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                || modelBlobTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            throw new RuntimeException("register type failed");
        }

        // 创建 Topic
        Topic trainCmdTopic = dp.create_topic(TOPIC_TRAIN_CMD,
                TrainCmdTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        Topic clientUpdateTopic = dp.create_topic(TOPIC_CLIENT_UPDATE,
                ClientUpdateTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        Topic modelBlobTopic = dp.create_topic(TOPIC_MODEL_BLOB,
                ModelBlobTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        publisher  = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        subscriber = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        trainCmdWriter = publisher.create_datawriter(trainCmdTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        modelBlobWriter = publisher.create_datawriter(modelBlobTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        clientUpdateReader = subscriber.create_datareader(clientUpdateTopic, Subscriber.DATAREADER_QOS_DEFAULT,
                new ClientUpdateListener(), StatusKind.DATA_AVAILABLE_STATUS);
    }

    // 发起一轮训练
    public void runTrainingRound(long subsetSize, int epochs, double lr, int seed) throws InterruptedException {
        int roundId = (int) roundCounter.getAndIncrement();
        System.out.println("[Controller] Starting round " + roundId);
        long tStart = System.currentTimeMillis();
        // 1) 发送 TrainCmd
        TrainCmd cmd = new TrainCmd();
        cmd.round_id   = roundId;
        cmd.subset_size= (int) subsetSize;
        cmd.epochs     = epochs;
        cmd.lr         = lr;
        cmd.seed       = seed;

        ReturnCode_t rc = ((TrainCmdDataWriter)trainCmdWriter).write(cmd, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller] Failed to write TrainCmd: " + rc);
            return;
        }

        // 2) 等待客户端返回 ClientUpdate（避免重复累计）
        List<ClientUpdate> collected = waitForClientUpdates(roundId, EXPECTED_CLIENTS, TIMEOUT_MS);
        if (collected.isEmpty()) {
            System.err.println("[Controller] No client updates received for round " + roundId);
            return;
        }
        System.out.println("[Controller] Collected " + collected.size() + " client updates.");

        // 3) 参数聚合（正确的 FedAvg：先解码为 float32 → 按样本数加权平均）
        float[] aggregated = aggregateFedAvg(collected);

        // 4) 发布聚合模型（以 FP32 小端字节发布；若以后要 INT8 下发，可再封装）
        byte[] aggregatedBytes = float32ToBytesLE(aggregated);
        ModelBlob blob = new ModelBlob();
        blob.round_id = roundId;
        blob.data = new Bytes();
        blob.data.loan_contiguous(aggregatedBytes, aggregatedBytes.length, aggregatedBytes.length); // 更高效
        rc = ((ModelBlobDataWriter)modelBlobWriter).write(blob, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller] Failed to write ModelBlob: " + rc);
        } else {
            System.out.println("[Controller] Published aggregated model for round " + roundId +
                    " bytes=" + aggregatedBytes.length);
        }

        long tEnd = System.currentTimeMillis();    // ✅ 计时结束
        long duration = tEnd - tStart;
        System.out.println("[e2e time] " + duration);

        // 5) 测试评估
        evaluateModel(aggregatedBytes);

        // 6) 清理本轮缓存
        updatesMap.remove(roundId);
    }

    private List<ClientUpdate> waitForClientUpdates(int roundId, int expectedClients, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        int lastCount = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            List<ClientUpdate> list = updatesMap.get(roundId);
            int cnt = (list == null) ? 0 : list.size();
            if (cnt != lastCount) {
                System.out.println("[Controller] progress: " + cnt + "/" + expectedClients);
                lastCount = cnt;
            }
            if (cnt >= expectedClients) break;
            Thread.sleep(100);
        }
        List<ClientUpdate> list = updatesMap.get(roundId);
        if (list == null) return Collections.emptyList();
        // 返回一个拷贝，避免外部修改
        return new ArrayList<>(list);
    }

    // ====== 正确的聚合：支持 FP32 原始/INT8 分块量化两种输入 ======
    private static float[] aggregateFedAvg(List<ClientUpdate> updates) {
        if (updates.isEmpty()) return new float[0];

        // 先把每个更新解码成 float32[]
        List<float[]> decoded = new ArrayList<>(updates.size());
        List<Long>    weights = new ArrayList<>(updates.size());
        int dim = -1;

        for (ClientUpdate cu : updates) {
            byte[] payload = new byte[cu.data.length()];
            for (int i = 0; i < payload.length; i++) payload[i] = cu.data.get_at(i);

            float[] vec = isQ8(payload) ? decodeQ8ToFloat(payload) : bytesToFloat32LE(payload);
            if (dim < 0) dim = vec.length;
            if (vec.length != dim) throw new IllegalStateException("inconsistent vector length among clients");
            decoded.add(vec);
            weights.add(cu.num_samples); // FedAvg 权重 = 样本数
        }

        double totalW = 0.0;
        for (Long w : weights) totalW += Math.max(0L, w);
        if (totalW <= 0) totalW = updates.size(); // 兜底：退化为等权

        float[] out = new float[dim];
        Arrays.fill(out, 0f);

        for (int i = 0; i < updates.size(); i++) {
            float[] v = decoded.get(i);
            double w = (weights.get(i) > 0 ? weights.get(i) : 1);
            float coef = (float) (w / totalW);
            for (int j = 0; j < dim; j++) {
                out[j] += v[j] * coef;
            }
        }
        return out;
    }

    // ====== 解码工具：FP32 小端 ======
    private static float[] bytesToFloat32LE(byte[] data) {
        if (data.length % 4 != 0) throw new IllegalArgumentException("fp32 bytes length not multiple of 4");
        int n = data.length / 4;
        float[] out = new float[n];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) out[i] = bb.getFloat();
        return out;
    }
    private static byte[] float32ToBytesLE(float[] v) {
        ByteBuffer bb = ByteBuffer.allocate(v.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f : v) bb.putFloat(f);
        return bb.array();
    }

    // ====== 解码工具：INT8 分块量化（Q8 头） ======
    private static boolean isQ8(byte[] data) {
        return data.length >= 3 && data[0] == 'Q' && data[1] == '8' && data[2] == 0;
    }
    private static float[] decodeQ8ToFloat(byte[] blob) {
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        byte m0 = bb.get(), m1 = bb.get(), m2 = bb.get();
        int ver  = bb.get() & 0xFF;
        if (m0 != 'Q' || m1 != '8' || m2 != 0 || ver != 1) {
            throw new IllegalArgumentException("bad INT8 blob header");
        }
        int  chunk     = bb.getInt();
        long totalL    = bb.getLong();
        int  nChunks   = bb.getInt();
        if (totalL > Integer.MAX_VALUE) throw new IllegalArgumentException("vector too large");
        int total = (int) totalL;

        float[] scales = new float[nChunks];
        for (int i = 0; i < nChunks; i++) scales[i] = bb.getFloat();

        float[] out = new float[total];
        for (int ci = 0; ci < nChunks; ci++) {
            int s = ci * chunk;
            int e = Math.min(s + chunk, total);
            float scale = scales[ci];
            for (int j = s; j < e; j++) {
                // Java byte -> signed [-128,127]
                byte q = bb.get();
                out[j] = q * scale;
            }
        }
        return out;
    }

    private void evaluateModel(byte[] modelData) {
        System.out.println("[Controller] Evaluating model, size=" + modelData.length);
        long t0 = System.currentTimeMillis();

        try {
            // 1. 写入临时模型文件
            Path tmpModel = Files.createTempFile("eval_model_", ".bin");
            Files.write(tmpModel, modelData);

            // 2. 构造命令行调用 Python
            List<String> cmd = new ArrayList<>();
            cmd.add("C:/Program Files/Python311/python.exe"); // 改为你的 python 路径
            cmd.add("E:/distributed-dds-ai-serving-system/distributed_training/evaluate_mnist.py"); // 改为你的脚本路径
            cmd.add("--model");      cmd.add(tmpModel.toString());
            cmd.add("--data_dir");   cmd.add("E:/distributed-dds-ai-serving-system/data");
            cmd.add("--batch_size"); cmd.add("32");
            // 不加 --int8 就表示不使用 INT8

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true); // 合并 stderr
            Process p = pb.start();

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println("[PY] " + line);
                    sb.append(line).append("\n");
                }
            }
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                System.err.println("[Controller] Evaluation script failed with exit code " + exitCode);
                return;
            }

            // 3. 提取准确率（可选：用正则提取）
            String allOut = sb.toString();
            double acc = parseAccuracy(allOut);
            long t1 = System.currentTimeMillis();
            System.out.printf("[Controller] Eval finished: Accuracy = %.4f, Time = %d ms\n", acc, t1 - t0);

            Files.deleteIfExists(tmpModel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static double parseAccuracy(String output) {
        try {
            int i = output.indexOf("Accuracy:");
            if (i >= 0) {
                String sub = output.substring(i);
                String[] parts = sub.split("[\\s(]");
                return Double.parseDouble(parts[1]);
            }
        } catch (Exception ignore) {}
        return -1;
    }

}
