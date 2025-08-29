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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class Client {

    // ====== 硬编码配置（按需修改） ======
    private static final int    DOMAIN_ID   = 200;
    private static final int    CLIENT_ID   = 1;
    private static final String PYTHON_EXE  = System.getenv().getOrDefault("PYTHON_EXE", "python");
    // 改成你的实际脚本路径：
    private static final String TRAINER_PY  = "E:/distributed-dds-ai-serving-system/distributed_training/train/dist_train.py";
    private static final String DATA_DIR    = "E:/distributed-dds-ai-serving-system/data";
    private static final int    BATCH_SIZE  = 128;
    // ====================================

    private DomainParticipant dp;
    private Publisher pub;
    private Subscriber sub;
    private Topic tCmd, tUpd, tModel;

    private TrainCmdDataReader cmdReader;
    private ModelBlobDataReader modelReader; // 可选
    private ClientUpdateDataWriter updWriter;

    private volatile int lastRound = -1;

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

        // 监听 TrainCmd
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
                    // 调 Python（入参走命令行），输出：二进制写文件；元信息（JSON）写 stdout
                    TrainResult tr = runPythonTraining(CLIENT_ID, seed, subsetSize, epochs, lr, BATCH_SIZE, DATA_DIR);

                    // 组装并发送 ClientUpdate
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

        // 可选：监听 ModelBlob（如果有下发模型）
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
        cmd.add("--client_id");   cmd.add(String.valueOf(clientId));
        cmd.add("--seed");        cmd.add(String.valueOf(seed));
        cmd.add("--subset");      cmd.add(String.valueOf(subset));
        cmd.add("--epochs");      cmd.add(String.valueOf(epochs));
        cmd.add("--lr");          cmd.add(Double.toString(lr));
        cmd.add("--batch_size");  cmd.add(String.valueOf(batchSize));
        cmd.add("--data_dir");    cmd.add(dataDir);
        cmd.add("--out");         cmd.add(outBin.toString());     // ← Python 写这个文件

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true); // 合并 stderr，方便看日志
        Process p = pb.start();

        // 读 stdout（Python 会打印 {"num_samples":..., "bytes":...}）
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[PY] " + line); // 打印原始日志
                sb.append(line);
            }
        }
        int code = p.waitFor();
        if (code != 0) throw new RuntimeException("dist_train.py exit=" + code);

        int numSamples = parseNumSamplesFromJson(sb.toString());

        byte[] bytes = Files.readAllBytes(outBin);
        try { Files.deleteIfExists(outBin); } catch (Exception ignore) {}

        return new TrainResult(numSamples, bytes);
    }

    // ——— 从一段（可能包含其它日志的）文本里，提取 {"num_samples": N } ———
    private static int parseNumSamplesFromJson(String text) {
        try {
            int l = text.indexOf('{');
            int r = text.lastIndexOf('}');
            if (l >= 0 && r > l) {
                String json = text.substring(l, r + 1);
                int i = json.indexOf("\"num_samples\"");
                if (i >= 0) {
                    int c = json.indexOf(':', i);
                    int d = json.indexOf(',', c);
                    String s = (d > 0 ? json.substring(c + 1, d) : json.substring(c + 1)).replaceAll("[^0-9]", "");
                    return Integer.parseInt(s);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0; // 兜底
    }

    // byte[] -> ai_train.Bytes（继承 ByteSeq：用 from_array/length）
    private static Bytes toBytes(byte[] raw) {
        Bytes out = new Bytes();
        if (raw != null) out.from_array(raw, raw.length);
        return out;
    }
    private static int bytesLen(Bytes b) { return (b == null) ? 0 : b.length(); }

    private static class TrainResult {
        final int numSamples;
        final byte[] bytes;
        TrainResult(int n, byte[] b) { numSamples = n; bytes = b; }
    }

    public static void main(String[] args) {
        Client node = new Client();
        java.util.concurrent.CountDownLatch quit = new java.util.concurrent.CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { node.shutdown(); } finally { quit.countDown(); }
        }));
        node.start();
        try { quit.await(); } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            node.shutdown();
        }
    }
}
