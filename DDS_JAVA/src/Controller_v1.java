import ai_train.Bytes;
import com.zrdds.infrastructure.*;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.*;

import ai_train.*;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controller_v1：控制端调度训练、收集客户端参数、聚合模型、评估（配置化）
 * 运行方式：
 *   java Controller_v1 path/to/config.json
 *
 * JSON 配置需要包含（示例）：
 * {
 *   "domain_id": 200,
 *   "expected_clients": 2,
 *   "timeout_ms": 60000,
 *   "subset_size": 5000,
 *   "epochs": 5,
 *   "lr": 0.01,
 *   "seed": 12345,
 *   "python_exe": "C:/Program Files/Python311/python.exe",
 *   "eval_script": "E:/distributed-dds-ai-serving-system/distributed_training/evaluate_mnist.py",
 *   "data_dir": "E:/distributed-dds-ai-serving-system/data",
 *   "batch_size": 32
 * }
 */
public class Controller_v1 {

    // ------- 配置对象 -------
    private static class Config {
        int domain_id;
        int expected_clients;
        long timeout_ms;
        long subset_size;
        int epochs;
        double lr;
        int seed;
        String python_exe;
        String eval_script;
        String data_dir;
        int batch_size;
    }

    // ------- 成员字段 -------
    private static Config config;

    // 每轮收到的客户端更新（round_id -> list）
    private static final ConcurrentMap<Integer, List<ClientUpdate>> updatesMap = new ConcurrentHashMap<>();

    private static final String TOPIC_TRAIN_CMD     = "train/train_cmd";
    private static final String TOPIC_CLIENT_UPDATE = "train/client_update";
    private static final String TOPIC_MODEL_BLOB    = "train/model_blob";

    private DomainParticipant dp;
    private Publisher publisher;
    private Subscriber subscriber;
    private DataWriter trainCmdWriter;
    private DataWriter modelBlobWriter;
    private DataReader clientUpdateReader;

    private final AtomicLong roundCounter = new AtomicLong(1);

    // ------- Listener 收集 ClientUpdate -------
    private static class ClientUpdateListener extends SimpleDataReaderListener<ClientUpdate, ClientUpdateSeq, ClientUpdateDataReader> {
        @Override
        public void on_process_sample(DataReader reader, ClientUpdate cu, SampleInfo info) {
            if (cu == null || info == null || !info.valid_data) return;
            updatesMap
                    .computeIfAbsent(cu.round_id, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(cu);
            System.out.println("[Controller] Received ClientUpdate: client=" + cu.client_id
                    + " round=" + cu.round_id
                    + " bytes=" + (cu.data == null ? 0 : cu.data.length())
                    + " nsamples=" + cu.num_samples);
        }
        @Override public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {}
    }

    // ------- 主程序入口 -------
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java Controller_v1 <config.json>");
            System.exit(1);
        }
        // 读取 JSON 配置（org.json）
        String jsonText = Files.readString(Paths.get(args[0]), StandardCharsets.UTF_8);
        JSONObject jo = new JSONObject(jsonText);
        config = new Config();
        config.domain_id        = jo.getInt("domain_id");
        config.expected_clients = jo.getInt("expected_clients");
        config.timeout_ms       = jo.getLong("timeout_ms");
        config.subset_size      = jo.getLong("subset_size");
        config.epochs           = jo.getInt("epochs");
        config.lr               = jo.getDouble("lr");
        config.seed             = jo.getInt("seed");
        config.python_exe       = jo.getString("python_exe");
        config.eval_script      = jo.getString("eval_script");
        config.data_dir         = jo.getString("data_dir");
        config.batch_size       = jo.getInt("batch_size");

        Controller_v1 ctrl = new Controller_v1();
        ctrl.init();
        ctrl.runTrainingRound();   // 如需多轮，可循环调用
        DDSIF.Finalize();
    }

    // ------- DDS 初始化 -------
    private void init() {
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        if (dpf == null) throw new RuntimeException("DDSIF.init failed");

        dp = dpf.create_participant(
                config.domain_id,
                DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE);
        if (dp == null) throw new RuntimeException("create participant failed");

        TrainCmdTypeSupport.get_instance().register_type(dp, null);
        ClientUpdateTypeSupport.get_instance().register_type(dp, null);
        ModelBlobTypeSupport.get_instance().register_type(dp, null);

        Topic tCmd   = dp.create_topic(TOPIC_TRAIN_CMD,     TrainCmdTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic tUpd   = dp.create_topic(TOPIC_CLIENT_UPDATE, ClientUpdateTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic tModel = dp.create_topic(TOPIC_MODEL_BLOB,    ModelBlobTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        publisher  = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        subscriber = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        trainCmdWriter   = publisher.create_datawriter(tCmd,   Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        modelBlobWriter  = publisher.create_datawriter(tModel, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        clientUpdateReader = subscriber.create_datareader(tUpd, Subscriber.DATAREADER_QOS_DEFAULT,
                new ClientUpdateListener(), StatusKind.DATA_AVAILABLE_STATUS);

        System.out.println("[Controller] DDS initialized.");
    }

    // ------- 发起一轮训练（端到端计时） -------
    public void runTrainingRound() throws InterruptedException {
        int roundId = (int) roundCounter.getAndIncrement();
        System.out.println("[Controller] Starting round " + roundId);
        long tStart = System.currentTimeMillis();

        // 1) 发送 TrainCmd
        TrainCmd cmd = new TrainCmd();
        cmd.round_id    = roundId;
        cmd.subset_size = (int) config.subset_size;
        cmd.epochs      = config.epochs;
        cmd.lr          = config.lr;
        cmd.seed        = config.seed;
        ReturnCode_t rc = ((TrainCmdDataWriter) trainCmdWriter).write(cmd, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller] Failed to write TrainCmd: " + rc);
            return;
        }

        // 2) 等待客户端返回
        List<ClientUpdate> collected = waitForClientUpdates(roundId, config.expected_clients, config.timeout_ms);
        if (collected.isEmpty()) {
            System.err.println("[Controller] No client updates received for round " + roundId);
            return;
        }
        System.out.println("[Controller] Collected " + collected.size() + " client updates.");

        // 3) FedAvg 聚合（先解码→按样本数加权）
        float[] aggregated = aggregateFedAvg(collected);

        // 4) 发布聚合模型（FP32 小端字节）
        byte[] aggregatedBytes = float32ToBytesLE(aggregated);
        ModelBlob blob = new ModelBlob();
        blob.round_id = roundId;
        blob.data = new Bytes();
        blob.data.loan_contiguous(aggregatedBytes, aggregatedBytes.length, aggregatedBytes.length);
        rc = ((ModelBlobDataWriter) modelBlobWriter).write(blob, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller] Failed to write ModelBlob: " + rc);
        } else {
            System.out.println("[Controller] Published aggregated model for round " + roundId
                    + " bytes=" + aggregatedBytes.length);
        }

        long tEnd = System.currentTimeMillis();
        System.out.println("[Controller] Round " + roundId + " end-to-end time: " + (tEnd - tStart) + " ms");

        // 5) 评估
        evaluateModel(aggregatedBytes);

        // 6) 清理
        updatesMap.remove(roundId);
    }

    // ------- 等待收集若干客户端更新（带超时） -------
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
        return new ArrayList<>(list); // 拷贝，避免外部改动
    }

    // ------- FedAvg：支持 FP32/INT8(Q8) 两种输入 -------
    private static float[] aggregateFedAvg(List<ClientUpdate> updates) {
        if (updates.isEmpty()) return new float[0];

        List<float[]> decoded = new ArrayList<>(updates.size());
        List<Long> weights = new ArrayList<>(updates.size());
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
        if (totalW <= 0) totalW = updates.size(); // 兜底为等权

        float[] out = new float[dim];
        Arrays.fill(out, 0f);
        for (int i = 0; i < decoded.size(); i++) {
            float[] v = decoded.get(i);
            double w = (weights.get(i) > 0 ? weights.get(i) : 1);
            float coef = (float) (w / totalW);
            for (int j = 0; j < dim; j++) {
                out[j] += v[j] * coef;
            }
        }
        return out;
    }

    // ------- 工具：FP32 小端编解码 -------
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

    // ------- 工具：识别/解码 INT8 分块量化（Q8 头） -------
    private static boolean isQ8(byte[] data) {
        return data.length >= 3 && data[0] == 'Q' && data[1] == '8' && data[2] == 0;
    }

    private static float[] decodeQ8ToFloat(byte[] blob) {
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        byte m0 = bb.get(), m1 = bb.get(), m2 = bb.get();
        int ver = bb.get() & 0xFF;
        if (m0 != 'Q' || m1 != '8' || m2 != 0 || ver != 1) {
            throw new IllegalArgumentException("bad INT8 blob header");
        }
        int chunk   = bb.getInt();
        long totalL = bb.getLong();
        int nChunks = bb.getInt();
        if (totalL > Integer.MAX_VALUE) throw new IllegalArgumentException("vector too large");
        int total = (int) totalL;

        // scales
        float[] scales = new float[nChunks];
        for (int i = 0; i < nChunks; i++) scales[i] = bb.getFloat();

        // q values → dequant
        float[] out = new float[total];
        for (int ci = 0; ci < nChunks; ci++) {
            int s = ci * chunk;
            int e = Math.min(s + chunk, total);
            float scale = scales[ci];
            for (int j = s; j < e; j++) {
                byte q = bb.get(); // signed [-128,127]
                out[j] = q * scale;
            }
        }
        return out;
    }

    // ------- 评估：调用 Python evaluate_mnist.py -------
    private void evaluateModel(byte[] modelData) {
        System.out.println("[Controller] Evaluating model, size=" + modelData.length);
        long t0 = System.currentTimeMillis();

        try {
            // 1) 将聚合后的 FP32 模型向量写临时文件
            Path tmpModel = Files.createTempFile("eval_model_", ".bin");
            Files.write(tmpModel, modelData);

            // 2) 组织命令行（批大小来自配置；不传 --int8）
            List<String> cmd = new ArrayList<>();
            cmd.add(config.python_exe);
            cmd.add(config.eval_script);
            cmd.add("--model");      cmd.add(tmpModel.toString());
            cmd.add("--data_dir");   cmd.add(config.data_dir);
            cmd.add("--batch_size"); cmd.add(String.valueOf(config.batch_size));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();

            // 3) 读取 stdout
            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println("[PY] " + line);
                    sb.append(line).append('\n');
                }
            }
            int exit = p.waitFor();
            if (exit != 0) {
                System.err.println("[Controller] Evaluation script exit code: " + exit);
                Files.deleteIfExists(tmpModel);
                return;
            }

            // 4) 提取精度（寻找 “Accuracy: <double>”）
            double acc = parseAccuracy(sb.toString());
            long t1 = System.currentTimeMillis();
            System.out.printf("[Controller] Eval finished: Accuracy=%.4f, Time=%d ms%n", acc, (t1 - t0));

            Files.deleteIfExists(tmpModel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static double parseAccuracy(String output) {
        try {
            // 简单解析模式：[Evaluate] Accuracy: 0.9845 (xxx/yyy)
            int i = output.indexOf("Accuracy:");
            if (i >= 0) {
                int j = i + "Accuracy:".length();
                // 跳过空格
                while (j < output.length() && Character.isWhitespace(output.charAt(j))) j++;
                // 读取数字
                int k = j;
                while (k < output.length() && (Character.isDigit(output.charAt(k)) || output.charAt(k)=='.')) k++;
                return Double.parseDouble(output.substring(j, k));
            }
        } catch (Exception ignore) {}
        return -1.0;
    }
}
