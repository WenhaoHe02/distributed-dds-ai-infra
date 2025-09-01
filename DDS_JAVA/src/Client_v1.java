import ai_train.*;  // TrainCmd, ClientUpdate, ModelBlob, Bytes, ...

import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.SampleInfo;
import com.zrdds.infrastructure.StatusKind;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class Client_v1 {

    // ====== 由配置文件加载的参数（见 loadConfig） ======
    private static int    DOMAIN_ID;
    private static int    CLIENT_ID;
    private static String PYTHON_EXE;
    private static String TRAINER_PY;
    private static String DATA_DIR;
    private static int    BATCH_SIZE;
    private static int    NUM_CLIENTS;
    private static int    INT8_CHUNK;
    private static String COMPRESS; // "int8" 或 "fp32"

    private DomainParticipant dp;
    private Publisher pub;
    private Subscriber sub;
    private Topic tCmd, tUpd, tModel;

    private TrainCmdDataReader cmdReader;
    private ModelBlobDataReader modelReader; // 可选
    private ClientUpdateDataWriter updWriter;

    private volatile int lastRound = -1;

    // ====== 入口：命令行传入配置文件路径 ======
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Client_v1 <client.conf.json>");
            return;
        }
        try {
            loadConfig(args[0]);
            Client_v1 node = new Client_v1();
            java.util.concurrent.CountDownLatch quit = new java.util.concurrent.CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { node.shutdown(); } finally { quit.countDown(); }
            }));
            node.start();
            try { quit.await(); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                node.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ====== 从 JSON 配置文件读取参数 ======
    private static void loadConfig(String confPath) throws Exception {
        String raw = Files.readString(Path.of(confPath), StandardCharsets.UTF_8);
        JSONObject j = new JSONObject(raw);
        DOMAIN_ID   = j.getInt("domain_id");
        CLIENT_ID   = j.getInt("client_id");
        // python_exe 可为空，支持用环境变量或默认 "python"
        PYTHON_EXE  = j.optString("python_exe", System.getenv().getOrDefault("PYTHON_EXE", "python"));
        TRAINER_PY  = j.getString("trainer_script");
        DATA_DIR    = j.getString("data_dir");
        BATCH_SIZE  = j.optInt("batch_size", 32);
        NUM_CLIENTS = j.optInt("num_clients", 1);
        INT8_CHUNK  = j.optInt("int8_chunk", 1024);
        COMPRESS    = j.optString("compress", "int8"); // 缺省 int8

        // 基本路径存在性提示（不阻断运行）
        if (!Files.exists(Path.of(TRAINER_PY))) {
            System.err.println("[WARN] trainer_script not found: " + TRAINER_PY);
        }
        if (!Files.isDirectory(Path.of(DATA_DIR))) {
            System.err.println("[WARN] data_dir not found: " + DATA_DIR);
        }
        System.out.println("[Client_v1] Loaded config: domain=" + DOMAIN_ID + ", client=" + CLIENT_ID +
                ", num_clients=" + NUM_CLIENTS + ", batch_size=" + BATCH_SIZE + ", compress=" + COMPRESS);
    }

    // ====== 启动并监听 ======
    public void start() {
        dp = DomainParticipantFactory.get_instance().create_participant(
                DOMAIN_ID, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        TrainCmdTypeSupport.get_instance().register_type(dp, null);
        ClientUpdateTypeSupport.get_instance().register_type(dp, null);
        ModelBlobTypeSupport.get_instance().register_type(dp, null);

        tCmd = dp.create_topic("train/train_cmd",
                TrainCmdTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        tUpd = dp.create_topic("train/client_update",
                ClientUpdateTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        tModel = dp.create_topic("train/model_blob",
                ModelBlobTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        updWriter = (ClientUpdateDataWriter) pub.create_datawriter(
                tUpd, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        cmdReader = (TrainCmdDataReader) sub.create_datareader(
                tCmd, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        modelReader = (ModelBlobDataReader) sub.create_datareader(
                tModel, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 监听 TrainCmd → 触发本地训练 → 回传 ClientUpdate
        cmdReader.set_listener(new SimpleDataReaderListener<TrainCmd, TrainCmdSeq, TrainCmdDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}

            @Override public void on_process_sample(DataReader r, TrainCmd cmd, SampleInfo info) {
                if (cmd == null || info == null || !info.valid_data) return;

                int round      = (int) cmd.round_id;
                int subsetSize = (int) cmd.subset_size;
                int epochs     = (int) cmd.epochs;
                double lr      = cmd.lr;
                int seed       = (int) cmd.seed;

                if (round <= lastRound) return;
                lastRound = round;

                System.out.println("[JavaDDS] TrainCmd: round=" + round +
                        " subset=" + subsetSize + " epochs=" + epochs + " lr=" + lr + " seed=" + seed);
                System.out.println("[JavaDDS] Using PYTHON_EXE: " + PYTHON_EXE);

                try {
                    long t0 = System.currentTimeMillis();
                    TrainResult tr = runPythonTraining(CLIENT_ID, seed, subsetSize, epochs, lr, BATCH_SIZE, DATA_DIR);
                    long t1 = System.currentTimeMillis();
                    System.out.println("[Client] local train cost: " + (t1 - t0) + " ms");

                    ClientUpdate upd = new ClientUpdate();
                    upd.client_id   = CLIENT_ID;
                    upd.round_id    = cmd.round_id;
                    upd.num_samples = (long) tr.numSamples;     // IDL: unsigned long long
                    upd.data        = toBytes(tr.bytes);        // byte[] -> ai_train.Bytes(ByteSeq)

                    updWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    System.out.println("[JavaDDS] sent ClientUpdate: round=" + round +
                            " n=" + tr.numSamples + " bytes=" + bytesLen(upd.data));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        // 可选：监听 ModelBlob（如果控制端下发聚合模型）
        modelReader.set_listener(new SimpleDataReaderListener<ModelBlob, ModelBlobSeq, ModelBlobDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}
            @Override public void on_process_sample(DataReader r, ModelBlob mb, SampleInfo info) {
                if (mb == null || info == null || !info.valid_data) return;
                System.out.println("[JavaDDS] ModelBlob: round=" + mb.round_id +
                        " bytes=" + bytesLen(mb.data));
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        System.out.println("[JavaDDS] client started. Waiting for TrainCmd...");
    }

    public void shutdown() {
        try { if (dp != null) dp.delete_contained_entities(); } catch (Exception ignore) {}
        System.out.println("[JavaDDS] shutdown.");
    }

    // === 调 Python：入参命令行；stdout 打 JSON（含 num_samples）；二进制写临时文件并读回 ===
    private TrainResult runPythonTraining(int clientId, int seed, int subset, int epochs, double lr,
                                          int batchSize, String dataDir) throws Exception {
        Path outBin = Files.createTempFile("upd_", ".bin");

        List<String> cmd = new ArrayList<>();
        cmd.add(PYTHON_EXE);
        cmd.add(TRAINER_PY);
        cmd.add("--client_id");    cmd.add(String.valueOf(clientId));
        cmd.add("--num_clients");  cmd.add(String.valueOf(NUM_CLIENTS));
        cmd.add("--seed");         cmd.add(String.valueOf(seed));
        if (subset > 0) { cmd.add("--subset"); cmd.add(String.valueOf(subset)); }
        cmd.add("--epochs");       cmd.add(String.valueOf(epochs));
        cmd.add("--lr");           cmd.add(Double.toString(lr));
        cmd.add("--batch_size");   cmd.add(String.valueOf(batchSize));
        cmd.add("--data_dir");     cmd.add(dataDir);
        // 压缩相关（可选）
        if ("int8".equalsIgnoreCase(COMPRESS)) {
            cmd.add("--compress");  cmd.add("int8");
            cmd.add("--chunk");     cmd.add(String.valueOf(INT8_CHUNK));
        } else if ("fp32".equalsIgnoreCase(COMPRESS)) {
            // 不传 compress 参数，按原始 fp32 导出
        }
        cmd.add("--out");          cmd.add(outBin.toString());     // ← Python 写这个文件

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true); // 合并 stderr，方便看日志
        Process p = pb.start();

        // 读 stdout（Python 建议打印一行 JSON：{"num_samples":..., "bytes":...}）
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[PY] " + line);
                sb.append(line).append('\n');
            }
        }
        int code = p.waitFor();
        if (code != 0) throw new RuntimeException("trainer exit=" + code);

        int numSamples = parseNumSamplesFromJson(sb.toString());

        byte[] bytes = Files.readAllBytes(outBin);
        try { Files.deleteIfExists(outBin); } catch (Exception ignore) {}

        return new TrainResult(numSamples, bytes);
    }

    // ——— 从输出文本里提取第一个 JSON 并读取 num_samples ———
    private static int parseNumSamplesFromJson(String text) {
        try {
            int l = text.indexOf('{');
            int r = text.lastIndexOf('}');
            if (l >= 0 && r > l) {
                String json = text.substring(l, r + 1);
                JSONObject obj = new JSONObject(json);
                if (obj.has("num_samples")) return obj.getInt("num_samples");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0; // 兜底
    }

    // byte[] -> ai_train.Bytes（继承 ByteSeq：loan_contiguous 更高效）
    private static Bytes toBytes(byte[] raw) {
        Bytes out = new Bytes();
        if (raw != null) out.loan_contiguous(raw, raw.length, raw.length);
        return out;
    }
    private static int bytesLen(Bytes b) { return (b == null) ? 0 : b.length(); }

    private static class TrainResult {
        final int numSamples;
        final byte[] bytes;
        TrainResult(int n, byte[] b) { numSamples = n; bytes = b; }
    }
}
