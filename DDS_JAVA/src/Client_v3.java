// Client_v3.java
// 规则：round==1 发送 FP32 整模型（单包）；round>=2 发送 S8 增量（默认 comm_every=1，可在 conf 里改）。
// 在 round>=2 前，先等一等 Controller 下发的最新聚合模型 latest_model.bin（默认等 8s）。

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

    private static int    INT8_CHUNK;
    private static int    SPARSE_K;
    private static double SPARSE_RATIO;
    private static int    COMM_EVERY;      // S8 模式下：1=每步，N=每N步

    private static String INIT_MODEL_PATH; // 可选，强制初始模型路径
    private static int    WAIT_MODEL_MS;   // 每轮开训前等待模型的最长时间（ms）

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

        INT8_CHUNK   = j.optInt("int8_chunk", 1024);
        SPARSE_K     = j.optInt("sparse_k", 0);
        SPARSE_RATIO = j.optDouble("sparse_ratio", 0.001);
        COMM_EVERY   = j.optInt("comm_every", 1);

        INIT_MODEL_PATH = j.optString("init_model_path", "").trim();
        WAIT_MODEL_MS   = j.optInt("wait_model_ms", 8000);

        System.out.println("[Client_v3] cfg: domain=" + DOMAIN_ID
                + " client=" + CLIENT_ID
                + " num_clients=" + NUM_CLIENTS
                + " sparse_k=" + SPARSE_K
                + " sparse_ratio=" + SPARSE_RATIO
                + " comm_every=" + COMM_EVERY
                + " wait_model_ms=" + WAIT_MODEL_MS
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
                    // round>=2 时等待聚合模型
                    waitForInitModelIfNeeded(round);
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
                            upd.num_samples = (i==total-1) ? (long) tr.numSamples : 0L;
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
        if (!INIT_MODEL_PATH.isEmpty()) return;      // 用户手工指定了路径，直接用
        if (round <= 1) return;                      // 第 1 轮没有上一轮模型
        long deadline = System.currentTimeMillis() + Math.max(0, WAIT_MODEL_MS);
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(latestModelPath)) { System.out.println("[Client_v3] found latest_model.bin for round "+round); return; }
            try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        System.out.println("[Client_v3] WARN: waited "+WAIT_MODEL_MS+"ms but still no model; start cold.");
    }

    public void shutdown() {
        try { if (dp != null) dp.delete_contained_entities(); } catch (Exception ignore) {}
        System.out.println("[Client_v3] shutdown.");
    }

    // —— 核心合并逻辑：round==1 强制 FP32；round>=2 强制 int8_sparse（comm_every=配置值） ——
    private TrainResult runPythonTraining(int clientId,int seed,int subset,int epochs,double lr,int batchSize,String dataDir,int round) throws Exception {
        final boolean firstRound = (round <= 1);
        String compressThisRound   = firstRound ? "fp32"       : "int8_sparse";
        int    commEveryThisRound  = firstRound ? 0            : COMM_EVERY;  // 首轮单包；后面按配置每N步

        boolean streamMode = "int8_sparse".equalsIgnoreCase(compressThisRound) && commEveryThisRound > 0;

        Path outDir = null, outBin = null;
        if (streamMode) outDir = Files.createTempDirectory("upd_stream_");
        else            outBin = Files.createTempFile("upd_", ".bin");

        List<String> cmd = new ArrayList<>();
        cmd.add(PYTHON_EXE); cmd.add(TRAINER_PY);
        cmd.add("--client_id");   cmd.add(String.valueOf(clientId));
        cmd.add("--num_clients"); cmd.add(String.valueOf(NUM_CLIENTS));
        cmd.add("--seed");        cmd.add(String.valueOf(seed));
        if (subset>0){ cmd.add("--subset"); cmd.add(String.valueOf(subset)); }
        cmd.add("--epochs");      cmd.add(String.valueOf(epochs));
        cmd.add("--lr");          cmd.add(Double.toString(lr));
        cmd.add("--batch_size");  cmd.add(String.valueOf(batchSize));
        cmd.add("--data_dir");    cmd.add(dataDir);
        cmd.add("--round");       cmd.add(String.valueOf(round));

        if ("int8_sparse".equalsIgnoreCase(compressThisRound)) {
            cmd.add("--compress"); cmd.add("int8_sparse");
            if (SPARSE_K > 0) { cmd.add("--sparse_k"); cmd.add(String.valueOf(SPARSE_K)); }
            else               { cmd.add("--sparse_ratio"); cmd.add(Double.toString(SPARSE_RATIO)); }
        } else if ("int8".equalsIgnoreCase(compressThisRound)) {
            cmd.add("--compress"); cmd.add("int8"); cmd.add("--chunk"); cmd.add(String.valueOf(INT8_CHUNK));
        } else {
            cmd.add("--compress"); cmd.add("fp32");
        }

        // init_model 优先顺序：init_model_path（若存在） > latest_model.bin（若存在） > 无
        Path initPath = null;
        if (!INIT_MODEL_PATH.isEmpty() && Files.exists(Paths.get(INIT_MODEL_PATH))) {
            initPath = Paths.get(INIT_MODEL_PATH);
        } else if (Files.exists(latestModelPath)) {
            initPath = latestModelPath;
        }
        if (initPath != null) {
            cmd.add("--init_model"); cmd.add(initPath.toAbsolutePath().toString());
            System.out.println("[Client_v3] init from model: " + initPath.toAbsolutePath());
        } else {
            System.out.println("[Client_v3] no init model found, cold start this round.");
        }

        // 输出与 comm_every
        if (streamMode) {
            cmd.add("--comm_every"); cmd.add(String.valueOf(commEveryThisRound));
            cmd.add("--out");        cmd.add(outDir.toString());
        } else {
            cmd.add("--comm_every"); cmd.add("0");
            cmd.add("--out");        cmd.add(outBin.toString());
        }

        String stateDir = Paths.get(dataDir, "client_"+clientId+"_state").toString();
        cmd.add("--state_dir"); cmd.add(stateDir);

        // 预留的 DGC/warmup 参数（用在 Python 侧）
        cmd.add("--dgc_momentum");      cmd.add("0.9");
        cmd.add("--dgc_clip_norm");     cmd.add("0.0");
        cmd.add("--dgc_mask_momentum"); cmd.add("1");
        cmd.add("--dgc_warmup_rounds"); cmd.add("1");

        ProcessBuilder pb = new ProcessBuilder(cmd); pb.redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder sb = new StringBuilder();
        try(BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))){
            String line; while((line=br.readLine())!=null){ System.out.println("[PY] "+line); sb.append(line).append('\n'); }
        }
        int code = p.waitFor(); if (code != 0) throw new RuntimeException("trainer exit="+code);

        int numSamples = parseNumSamplesFromJson(sb.toString());
        if (streamMode) {
            List<Path> files = listS8Files(outDir);
            List<byte[]> packets = new ArrayList<>(files.size());
            for (Path f: files) packets.add(Files.readAllBytes(f));
            safeDeleteDir(outDir);
            return TrainResult.stream(numSamples, packets);
        } else {
            byte[] bytes = Files.readAllBytes(outBin);
            try { Files.deleteIfExists(outBin); } catch (Exception ignore) {}
            return TrainResult.single(numSamples, bytes);
        }
    }

    private static List<Path> listS8Files(Path dir) throws Exception {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.s8")) {
            List<Path> list = new ArrayList<>(); for (Path p: ds) list.add(p);
            list.sort(Comparator.comparing(Path::getFileName)); return list;
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
