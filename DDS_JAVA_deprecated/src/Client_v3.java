// Client_v3.java
// 规则：round==1 默认可发送 FP32 整模型；若 sparse_first_round=true 则首轮也发送 S4（FP32 稀疏 Δ）。
// comm_every 语义：>0=每N步流式；==0=每epoch流式；<0=整轮单包。
// 在需要上一轮模型初始化时（非首轮或未启用首轮稀疏），先等 Controller 下发 latest_model.bin（默认等 8s）。

import ai_train.*;
import ai_train.Bytes;

import com.zrdds.domain.*;
import com.zrdds.infrastructure.*;
import com.zrdds.publication.*;
import com.zrdds.subscription.*;
import com.zrdds.topic.*;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Client_v3 {

    // ===== 配置 =====
    private static int    DOMAIN_ID;
    private static int    CLIENT_ID;
    private static int    NUM_CLIENTS;
    private static int    BATCH_SIZE;
    private static String DATA_DIR;
    private static String PYTHON_EXE;
    private static String TRAINER_PY;

    private static int    SPARSE_K;         // 保留字段（未用）
    private static double SPARSE_RATIO;     // 稀疏率（默认 0.1 = 10%）
    private static int    COMM_EVERY;       // >0 每N步；=0 每epoch；<0 单包
    private static boolean SPARSE_FIRST_ROUND; // 首轮是否也用稀疏

    private static String INIT_MODEL_PATH;  // 可选，强制初始模型路径
    private static int    WAIT_MODEL_MS;    // 每轮开训前等待模型的最长时间（ms）

    // DDS
    private DomainParticipant dp;
    private Publisher pub;
    private Subscriber sub;
    private Topic tCmd, tUpd, tModel;

    private TrainCmdDataReader cmdReader;
    private ModelBlobDataReader modelReader;
    private ClientUpdateDataWriter updWriter;

    private volatile int lastRound = -1;

    // 最新聚合模型保存处
    private final Path latestModelDir  = Paths.get(System.getProperty("user.dir"), "global_model");
    private final Path latestModelPath = latestModelDir.resolve("latest_model.bin");
    private final AtomicInteger latestModelRound = new AtomicInteger(-1);

    public static void main(String[] args) {
        if (args.length != 1) { System.err.println("Usage: java Client_v3 <client_v3.conf.json>"); return; }
        try {
            loadConfig(args[0]);
            Client_v3 node = new Client_v3();
            java.util.concurrent.CountDownLatch quit = new java.util.concurrent.CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { node.shutdown(); } finally { quit.countDown(); } }));
            node.start();
            try { quit.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); node.shutdown(); }
        } catch (Exception e) { e.printStackTrace(); }
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

        SPARSE_K     = j.optInt("sparse_k", 0);
        SPARSE_RATIO = j.optDouble("sparse_ratio", 0.1);
        COMM_EVERY   = j.optInt("comm_every", 0); // 默认每epoch流式
        SPARSE_FIRST_ROUND = j.optBoolean("sparse_first_round", false);

        INIT_MODEL_PATH = j.optString("init_model_path", "").trim();
        WAIT_MODEL_MS   = j.optInt("wait_model_ms", 8000);

        System.out.println("[Client_v3] cfg: domain=" + DOMAIN_ID
                + " client=" + CLIENT_ID
                + " num_clients=" + NUM_CLIENTS
                + " sparse_k=" + SPARSE_K
                + " sparse_ratio=" + SPARSE_RATIO
                + " comm_every=" + COMM_EVERY
                + " wait_model_ms=" + WAIT_MODEL_MS
                + " sparse_first_round=" + SPARSE_FIRST_ROUND
                + (INIT_MODEL_PATH.isEmpty() ? "" : " init_model_path="+INIT_MODEL_PATH));
    }

    public void start() {
        try { Files.createDirectories(latestModelDir); } catch (Exception ignore) {}

        dp = DomainParticipantFactory.get_instance().create_participant(
                DOMAIN_ID, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        TrainCmdTypeSupport.get_instance().register_type(dp, null);
        ClientUpdateTypeSupport.get_instance().register_type(dp, null);
        ModelBlobTypeSupport.get_instance().register_type(dp, null);

        tCmd = dp.create_topic("train/train_cmd", TrainCmdTypeSupport.get_instance().get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        tUpd = dp.create_topic("train/client_update", ClientUpdateTypeSupport.get_instance().get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        tModel = dp.create_topic("train/model_blob", ModelBlobTypeSupport.get_instance().get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        DataWriterQos wq = new DataWriterQos();
        pub.get_default_datawriter_qos(wq);
        wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        wq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        wq.history.depth    = 8;
        updWriter = (ClientUpdateDataWriter) pub.create_datawriter(tUpd, wq, null, StatusKind.STATUS_MASK_NONE);
        try { waitWriterMatched(updWriter, 1, 5000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

        DataReaderQos rq = new DataReaderQos();
        sub.get_default_datareader_qos(rq);
        rq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        rq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        rq.history.depth    = 8;

        cmdReader = (TrainCmdDataReader) sub.create_datareader(tCmd, rq, null, StatusKind.STATUS_MASK_NONE);
        try { waitReaderMatched(cmdReader, 1, 5000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

        modelReader = (ModelBlobDataReader) sub.create_datareader(tModel, rq, null, StatusKind.STATUS_MASK_NONE);
        try { waitReaderMatched(modelReader, 1, 2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

        cmdReader.set_listener(new SimpleDataReaderListener<TrainCmd, TrainCmdSeq, TrainCmdDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}
            @Override public void on_process_sample(DataReader r, TrainCmd cmd, SampleInfo info) {
                if (cmd == null || info == null || !info.valid_data) return;
                int round=(int)cmd.round_id, subset=(int)cmd.subset_size, epochs=(int)cmd.epochs, seed=(int)cmd.seed;
                double lr=cmd.lr;
                if (round <= lastRound) return; lastRound = round;

                System.out.println("[Client_v3] TrainCmd: round="+round+" subset="+subset+" epochs="+epochs+" lr="+lr+" seed="+seed);
                try {
                    // 若首轮走稀疏（不依赖上一轮模型），就不等待；其他情况按需等待上一轮模型
                    boolean wantSparse = (SPARSE_FIRST_ROUND || round > 1);
                    if (!(wantSparse && round <= 1)) {  // 非“首轮稀疏”才等待
                        waitForInitModelIfNeeded(round);
                    }

                    long t0 = System.currentTimeMillis();
                    TrainResult tr = runPythonTraining(CLIENT_ID, seed, subset, epochs, lr, BATCH_SIZE, DATA_DIR, round);
                    long t1 = System.currentTimeMillis();
                    System.out.println("[Client_v3] local train+pack cost: " + (t1 - t0) + " ms");

                    if (tr.isStream()) {
                        int total = tr.packets.size();
                        for (int i=0;i<total;i++){
                            byte[] pkt = tr.packets.get(i);
                            ClientUpdate upd = new ClientUpdate();
                            upd.client_id=CLIENT_ID; upd.round_id=cmd.round_id;
                            upd.num_samples = (i==total-1) ? (long) tr.numSamples : 0L; // 最后一包携带样本数
                            upd.data = toBytes(pkt);
                            updWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        }
                        System.out.println("[Client_v3] sent stream packets: "+total);
                    } else {
                        ClientUpdate upd = new ClientUpdate();
                        upd.client_id=CLIENT_ID; upd.round_id=cmd.round_id;
                        upd.num_samples=(long)tr.numSamples; upd.data=toBytes(tr.singleBytes);
                        updWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
                        System.out.println("[Client_v3] sent single update bytes="+bytesLen(upd.data));
                    }
                } catch (Exception e) { e.printStackTrace(); }
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        modelReader.set_listener(new SimpleDataReaderListener<ModelBlob, ModelBlobSeq, ModelBlobDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}
            @Override public void on_process_sample(DataReader r, ModelBlob mb, SampleInfo info) {
                if (mb == null || info == null || !info.valid_data) return;
                try {
                    byte[] buf = bytesToArray(mb.data);
                    Files.createDirectories(latestModelDir);
                    Files.write(latestModelPath, buf, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                    latestModelRound.set((int) mb.round_id);
                    System.out.println("[Client_v3] ModelBlob: round="+mb.round_id+" -> "+latestModelPath.toAbsolutePath());
                } catch (Exception e) { System.err.println("[Client_v3] failed to save ModelBlob: "+e); }
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        System.out.println("[Client_v3] started. Waiting for TrainCmd...");
    }

    private void waitForInitModelIfNeeded(int round){
        if (!INIT_MODEL_PATH.isEmpty()) return; // 用户手工指定：跳过等待
        if (round <= 1) return;                 // 首轮无上一轮模型

        final int needRound = round - 1;
        System.out.println("[Client_v3] waiting global model for prev round = " + needRound);
        long maxWaitMs = Math.max(600, WAIT_MODEL_MS);  // 至少 60s
        long start = System.currentTimeMillis();

        while (true) {
            int have = latestModelRound.get();
            if (have >= needRound && Files.exists(latestModelPath)) {
                System.out.println("[Client_v3] found latest_model.bin for round " + needRound);
                return;
            }
            if (System.currentTimeMillis() - start > maxWaitMs) {
                System.out.println("[Client_v3] WARN: waited " + (System.currentTimeMillis()-start)
                        + "ms but still no model for round " + needRound + "; start cold.");
                return;
            }
            try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
        }
    }

    public void shutdown() {
        try { if (dp != null) dp.delete_contained_entities(); } catch (Exception ignore) {}
        System.out.println("[Client_v3] shutdown.");
    }

    // —— 压缩/窗口参数传给 Python： --compress, --sparse_ratio, --comm_every, --subset ——
    private TrainResult runPythonTraining(int clientId,int seed,int subset,int epochs,double lr,int batchSize,String dataDir,int round) throws Exception {
        final boolean wantSparse = (SPARSE_FIRST_ROUND || round > 1);
        String compressThisRound = wantSparse ? "fp32_sparse" : "fp32_full";

        // 决定 init_model（可选）
        Path initPath = null;
        if (!INIT_MODEL_PATH.isEmpty() && Files.exists(Paths.get(INIT_MODEL_PATH))) {
            initPath = Paths.get(INIT_MODEL_PATH);
        } else if (Files.exists(latestModelPath)) {
            initPath = latestModelPath;
        }
        // 注意：首轮稀疏且拿不到 init 时，不回退为 FULL；由 Python 端使用“相对零权重”的 Δ 语义。

        // 流式判定：fp32_sparse 且 COMM_EVERY >= 0
        final boolean streamMode = "fp32_sparse".equalsIgnoreCase(compressThisRound) && (COMM_EVERY >= 0);

        List<String> cmd = new ArrayList<>();
        cmd.add(PYTHON_EXE);
        cmd.add(TRAINER_PY);

        Path outDir = null, outBin = null;
        if (streamMode) {
            outDir = Files.createTempDirectory("upd_stream_");
            cmd.add("--out"); cmd.add(outDir.toString());
        } else {
            outBin = Files.createTempFile("upd_", ".bin");
            cmd.add("--out"); cmd.add(outBin.toString());
        }

        // 训练参数
        cmd.add("--client_id");   cmd.add(String.valueOf(clientId));
        cmd.add("--num_clients"); cmd.add(String.valueOf(NUM_CLIENTS));
        cmd.add("--seed");        cmd.add(String.valueOf(seed));
        cmd.add("--epochs");      cmd.add(String.valueOf(epochs));
        cmd.add("--lr");          cmd.add(Double.toString(lr));
        cmd.add("--batch_size");  cmd.add(String.valueOf(batchSize));
        cmd.add("--data_dir");    cmd.add(dataDir);
        cmd.add("--round");       cmd.add(String.valueOf(round));
        cmd.add("--subset");      cmd.add(String.valueOf(subset));

        // 压缩与窗口参数
        cmd.add("--compress");      cmd.add(compressThisRound);
        cmd.add("--sparse_ratio");  cmd.add(Double.toString(SPARSE_RATIO));
        cmd.add("--comm_every");    cmd.add(Integer.toString(COMM_EVERY));

        // init_model（若有）
        if (initPath != null) {
            cmd.add("--init_model"); cmd.add(initPath.toAbsolutePath().toString());
            System.out.println("[Client_v3] init from model: " + initPath.toAbsolutePath());
        } else {
            System.out.println("[Client_v3] no init model found, cold start this round.");
        }

        // 状态目录（保留参数位；脚本未使用）
        String stateDir = Paths.get(dataDir, "client_" + clientId + "_state").toString();
        cmd.add("--state_dir"); cmd.add(stateDir);

        // 运行
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line; while ((line = br.readLine()) != null) { System.out.println("[PY] " + line); sb.append(line).append('\n'); }
        }
        int code = p.waitFor();
        if (code != 0) throw new RuntimeException("trainer exit=" + code);

        int numSamples = parseNumSamplesFromJson(sb.toString());

        if (streamMode) {
            List<Path> files = listS4Files(outDir);
            List<byte[]> packets = new ArrayList<>(files.size());
            for (Path f : files) packets.add(Files.readAllBytes(f));
            safeDeleteDir(outDir);
            return TrainResult.stream(numSamples, packets);
        } else {
            byte[] bytes = Files.readAllBytes(outBin);
            try { Files.deleteIfExists(outBin); } catch (Exception ignore) {}
            return TrainResult.single(numSamples, bytes);
        }
    }

    private static List<Path> listS4Files(Path dir) throws Exception {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.s4")) {
            List<Path> list = new ArrayList<>();
            for (Path p: ds) list.add(p);
            list.sort(Comparator.comparing(Path::getFileName)); // 00001.s4, 00002.s4, ...
            return list;
        }
    }
    private static void safeDeleteDir(Path dir){
        try{
            if (dir==null || !Files.exists(dir)) return;
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} });
        }catch(Exception ignore){}
    }

    private static int parseNumSamplesFromJson(String text){
        try{
            int l=text.indexOf('{'), r=text.lastIndexOf('}');
            if (l>=0 && r>l){ String json=text.substring(l, r+1); JSONObject obj=new JSONObject(json); if(obj.has("num_samples")) return obj.getInt("num_samples"); }
        }catch(Exception e){ e.printStackTrace(); }
        return 0;
    }
    private static Bytes toBytes(byte[] raw){ Bytes out=new Bytes(); if(raw!=null) out.loan_contiguous(raw, raw.length, raw.length); return out; }
    private static int bytesLen(Bytes b){ return (b==null)?0:b.length(); }
    private static byte[] bytesToArray(Bytes b){ if(b==null) return new byte[0]; int n=b.length(); byte[] out=new byte[n]; b.to_array(out,n); return out; }

    private static boolean waitWriterMatched(DataWriter writer, int minMatches, long timeoutMs) throws InterruptedException {
        long start=System.currentTimeMillis(); PublicationMatchedStatus st=new PublicationMatchedStatus(); int last=-1;
        while(System.currentTimeMillis()-start<timeoutMs){
            ReturnCode_t rc=writer.get_publication_matched_status(st);
            if(rc!=ReturnCode_t.RETCODE_OK){ System.err.println("[Client_v3] writer status rc="+rc); return false; }
            if(st.current_count!=last){ System.out.println("[Client_v3] updWriter matched: current="+st.current_count); last=st.current_count; }
            if(st.current_count>=minMatches) return true; Thread.sleep(100);
        } return false;
    }
    private static boolean waitReaderMatched(DataReader reader, int minMatches, long timeoutMs) throws InterruptedException {
        long start=System.currentTimeMillis(); SubscriptionMatchedStatus st=new SubscriptionMatchedStatus(); int last=-1;
        while(System.currentTimeMillis()-start<timeoutMs){
            ReturnCode_t rc=reader.get_subscription_matched_status(st);
            if(rc!=ReturnCode_t.RETCODE_OK){ System.err.println("[Client_v3] reader status rc="+rc); return false; }
            if(st.current_count!=last){ System.out.println("[Client_v3] reader matched: current="+st.current_count); last=st.current_count; }
            if(st.current_count>=minMatches) return true; Thread.sleep(100);
        } return false;
    }

    private static class TrainResult {
        final int numSamples; final byte[] singleBytes; final List<byte[]> packets;
        private TrainResult(int n, byte[] one, List<byte[]> many){ numSamples=n; singleBytes=one; packets=many; }
        static TrainResult single(int n, byte[] b){ return new TrainResult(n,b,null); }
        static TrainResult stream(int n, List<byte[]> list){ return new TrainResult(n,null,list); }
        boolean isStream(){ return packets!=null; }
    }
}
