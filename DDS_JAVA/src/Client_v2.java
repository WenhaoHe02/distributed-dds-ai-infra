import ai_train.*;  // TrainCmd, ClientUpdate, ModelBlob, Bytes, ...

import ai_train.Bytes;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.*;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;
import com.zrdds.publication.DataWriterQos;
import com.zrdds.subscription.DataReaderQos;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Client_v2 {

    // ====== 由配置文件加载 ======
    private static int    DOMAIN_ID;
    private static int    CLIENT_ID;
    private static int    NUM_CLIENTS;
    private static int    BATCH_SIZE;
    private static String DATA_DIR;
    private static String PYTHON_EXE;
    private static String TRAINER_PY;

    // 压缩/稀疏相关（客户端控制）
    private static String COMPRESS;        // "int8_sparse" | "int8" | "fp32"
    private static int    INT8_CHUNK;      // dense int8 时使用（兼容旧路径）
    private static int    SPARSE_K;        // 若 >0，使用 top-k
    private static double SPARSE_RATIO;    // 若 >0，使用比例（0.001=0.1%）

    private DomainParticipant dp;
    private Publisher pub;
    private Subscriber sub;
    private Topic tCmd, tUpd, tModel;

    private TrainCmdDataReader cmdReader;
    private ModelBlobDataReader modelReader; // 可选
    private ClientUpdateDataWriter updWriter;

    private volatile int lastRound = -1;

    // 新增：保存从 Controller 收到的聚合模型，供下一轮初始化
    private final Path latestModelDir = Paths.get(System.getProperty("user.dir"), "global_model");
    private final Path latestModelPath = latestModelDir.resolve("latest_model.bin");
    private final AtomicInteger latestModelRound = new AtomicInteger(-1);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Client_v2 <client_v2.conf.json>");
            return;
        }
        try {
            loadConfig(args[0]);
            Client_v2 node = new Client_v2();
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

    private static void loadConfig(String confPath) throws Exception {
        String raw = Files.readString(Path.of(confPath), StandardCharsets.UTF_8);
        JSONObject j = new JSONObject(raw);

        DOMAIN_ID   = j.getInt("domain_id");
        CLIENT_ID   = j.getInt("client_id");
        NUM_CLIENTS = j.getInt("num_clients");
        BATCH_SIZE  = j.optInt("batch_size", 32);
        DATA_DIR    = j.getString("data_dir");
        PYTHON_EXE  = j.optString("python_exe", System.getenv().getOrDefault("PYTHON_EXE", "python"));
        TRAINER_PY  = j.getString("trainer_script");

        COMPRESS     = j.optString("compress", "fp32");
        INT8_CHUNK   = j.optInt("int8_chunk", 1024);           // dense int8 兼容项
        SPARSE_K     = j.optInt("sparse_k", 0);
        SPARSE_RATIO = j.optDouble("sparse_ratio", 0.001);     // 0.1%

        System.out.println("[Client_v2] cfg: domain=" + DOMAIN_ID
                + " client=" + CLIENT_ID
                + " num_clients=" + NUM_CLIENTS
                + " compress=" + COMPRESS
                + " sparse_k=" + SPARSE_K
                + " sparse_ratio=" + SPARSE_RATIO);
    }

    public void start() {
        try { Files.createDirectories(latestModelDir); } catch (Exception ignore) {}

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
        DataWriterQos wq = new DataWriterQos();
        pub.get_default_datawriter_qos(wq);
        wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        wq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        wq.history.depth    = 32;
        updWriter = (ClientUpdateDataWriter) pub.create_datawriter(
                tUpd, wq, null, StatusKind.STATUS_MASK_NONE);
        try {
            boolean mw = waitWriterMatched(updWriter, 1, 5000);
            System.out.println("[Client_v2] updWriter initially matched=" + mw);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        DataReaderQos rq = new DataReaderQos();
        sub.get_default_datareader_qos(rq);
        rq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        rq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        rq.history.depth    = 32;

        cmdReader = (TrainCmdDataReader) sub.create_datareader(
                tCmd, rq, null, StatusKind.STATUS_MASK_NONE);
        try {
            boolean mr = waitReaderMatched(cmdReader, 1, 5000);
            System.out.println("[Client_v2] cmdReader matched=" + mr);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        modelReader = (ModelBlobDataReader) sub.create_datareader(
                tModel, rq, null, StatusKind.STATUS_MASK_NONE);
        try {
            boolean mr2 = waitReaderMatched(modelReader, 1, 2000);
            System.out.println("[Client_v2] modelReader matched=" + mr2 + " (first boot may be false)");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
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

                System.out.println("[Client_v2] TrainCmd: round=" + round +
                        " subset=" + subsetSize + " epochs=" + epochs + " lr=" + lr + " seed=" + seed);
                System.out.println("[Client_v2] Python: " + PYTHON_EXE + "  Trainer: " + TRAINER_PY);

                try {
                    long t0 = System.currentTimeMillis();
                    TrainResult tr = runPythonTraining(CLIENT_ID, seed, subsetSize, epochs, lr, BATCH_SIZE, DATA_DIR, round);
                    long t1 = System.currentTimeMillis();
                    System.out.println("[Client_v2] local train+pack cost: " + (t1 - t0) + " ms");

                    ClientUpdate upd = new ClientUpdate();
                    upd.client_id   = CLIENT_ID;
                    upd.round_id    = cmd.round_id;
                    upd.num_samples = (long) tr.numSamples; // unsigned long long
                    upd.data        = toBytes(tr.bytes);

                    updWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    System.out.println("[Client_v2] sent ClientUpdate: round=" + round +
                            " n=" + tr.numSamples + " bytes=" + bytesLen(upd.data));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        // 监听 ModelBlob：保存为 latest_model.bin 供下一轮初始化
        modelReader.set_listener(new SimpleDataReaderListener<ModelBlob, ModelBlobSeq, ModelBlobDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}

            @Override public void on_process_sample(DataReader r, ModelBlob mb, SampleInfo info) {
                if (mb == null || info == null || !info.valid_data) return;
                try {
                    byte[] buf = bytesToArray(mb.data);
                    Files.write(latestModelPath, buf, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                    latestModelRound.set((int) mb.round_id);
                    System.out.println("[Client_v2] ModelBlob: round=" + mb.round_id +
                            " bytes=" + bytesLen(mb.data) + " -> saved to " + latestModelPath.toAbsolutePath());
                } catch (Exception e) {
                    System.err.println("[Client_v2] failed to save ModelBlob: " + e);
                }
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        System.out.println("[Client_v2] started. Waiting for TrainCmd...");
    }

    public void shutdown() {
        try { if (dp != null) dp.delete_contained_entities(); } catch (Exception ignore) {}
        System.out.println("[Client_v2] shutdown.");
    }

    // 调 Python：stdout 打 JSON（含 num_samples）；二进制写临时文件并读回
    private TrainResult runPythonTraining(int clientId, int seed, int subset, int epochs, double lr,
                                          int batchSize, String dataDir, int round) throws Exception {
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
        cmd.add("--round");        cmd.add(String.valueOf(round));
        // 压缩/稀疏控制参数
        if ("int8_sparse".equalsIgnoreCase(COMPRESS)) {
            cmd.add("--compress"); cmd.add("int8_sparse");
            if (SPARSE_K > 0) {
                cmd.add("--sparse_k"); cmd.add(String.valueOf(SPARSE_K));
            } else if (SPARSE_RATIO > 0.0) {
                cmd.add("--sparse_ratio"); cmd.add(Double.toString(SPARSE_RATIO));
            }
        } else if ("int8".equalsIgnoreCase(COMPRESS)) {
            cmd.add("--compress"); cmd.add("int8");
            cmd.add("--chunk");    cmd.add(String.valueOf(INT8_CHUNK));
        } else {
            cmd.add("--compress"); cmd.add("fp32");
        }

        // DGC 参数（可按需改为配置读取）
        cmd.add("--dgc_momentum");      cmd.add("0.9");
        cmd.add("--dgc_clip_norm");     cmd.add("0.0");
        cmd.add("--dgc_mask_momentum"); cmd.add("1");
        cmd.add("--dgc_warmup_rounds"); cmd.add("1");

        // === 新增：如存在最新聚合模型，则传给 Python 作为初始化 ===
        if (Files.exists(latestModelPath)) {
            cmd.add("--init_model");
            cmd.add(latestModelPath.toAbsolutePath().toString());
            System.out.println("[Client_v2] init from model: " + latestModelPath.toAbsolutePath());
        } else {
            System.out.println("[Client_v2] no init model found, cold start this round.");
        }

        cmd.add("--out");          cmd.add(outBin.toString());
        String stateDir = Paths.get(dataDir, "client_" + clientId + "_state").toString();
        cmd.add("--state_dir"); cmd.add(stateDir);

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();

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
        byte[] bytes   = Files.readAllBytes(outBin);

        // 清理输出文件
        try { Files.deleteIfExists(outBin); } catch (Exception ignore) {}

        return new TrainResult(numSamples, bytes);
    }

    private static int parseNumSamplesFromJson(String text) {
        try {
            int l = text.indexOf('{');
            int r = text.lastIndexOf('}');
            if (l >= 0 && r > l) {
                String json = text.substring(l, r + 1);
                JSONObject obj = new JSONObject(json);
                if (obj.has("num_samples")) return obj.getInt("num_samples");
            }
        } catch (Exception e) { e.printStackTrace(); }
        return 0;
    }

    private static Bytes toBytes(byte[] raw) {
        Bytes out = new Bytes();
        if (raw != null) out.loan_contiguous(raw, raw.length, raw.length);
        return out;
    }
    private static int bytesLen(Bytes b) { return (b == null) ? 0 : b.length(); }

    private static byte[] bytesToArray(Bytes b) {
        if (b == null) return new byte[0];
        int n = b.length();
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) out[i] = b.get_at(i);
        return out;
    }

    private static class TrainResult {
        final int numSamples;
        final byte[] bytes;
        TrainResult(int n, byte[] b) { numSamples = n; bytes = b; }
    }

    private static boolean waitWriterMatched(ClientUpdateDataWriter writer, int minMatches, long timeoutMs)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        PublicationMatchedStatus st = new PublicationMatchedStatus();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ReturnCode_t rc = writer.get_publication_matched_status(st);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Client_v2] get_publication_matched_status rc=" + rc);
                return false;
            }
            if (st.current_count != last) {
                System.out.println("[Client_v2] updWriter matched: current=" + st.current_count
                        + " total=" + st.total_count
                        + " change=" + st.current_count_change);
                last = st.current_count;
            }
            if (st.current_count >= minMatches) return true;
            Thread.sleep(100);
        }
        return false;
    }

    /** 等待 DataReader 至少匹配到 minMatches 个 DataWriter，并打印状态 */
    private static boolean waitReaderMatched(DataReader reader, int minMatches, long timeoutMs)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        SubscriptionMatchedStatus st = new SubscriptionMatchedStatus();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ReturnCode_t rc = reader.get_subscription_matched_status(st);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Client_v2] get_subscription_matched_status rc=" + rc);
                return false;
            }
            if (st.current_count != last) {
                System.out.println("[Client_v2] reader matched: current=" + st.current_count
                        + " total=" + st.total_count
                        + " change=" + st.current_count_change);
                last = st.current_count;
            }
            if (st.current_count >= minMatches) return true;
            Thread.sleep(100);
        }
        return false;
    }

    /** 调试：判断负载魔数 */
    private static String magicOf(byte[] b) {
        if (b == null) return "null";
        if (b.length < 4) return "short(" + b.length + ")";
        int b0 = b[0] & 0xFF, b1 = b[1] & 0xFF, b2 = b[2] & 0xFF, b3 = b[3] & 0xFF;
        if (b0=='S' && b1=='8' && b2==0 && b3==1) return "S8/v1";
        if (b0=='Q' && b1=='8' && b2==0 && b3==1) return "Q8/v1";
        if (b.length % 4 == 0) return "FP32(?)";
        return String.format("??(%02X %02X %02X %02X)", b0,b1,b2,b3);
    }

    /** 可选：看前 8 字节十六进制 */
    private static String hexHead(byte[] b) {
        if (b == null) return "null";
        int n = Math.min(8, b.length);
        StringBuilder sb = new StringBuilder();
        for (int i=0;i<n;i++) sb.append(String.format("%02X ", b[i]));
        return sb.toString().trim();
    }

}
