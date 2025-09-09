// Controller_v3.java
// 语义：S4 = “模型增量 Δ（FP32 稀疏）”（同一客户端多包先相加 → FedAvg → currentModel += Δ）
//      FP32 = “完整权重”（FedAvg → 覆盖 currentModel）
// 默认每轮发布 FP32 模型给客户端初始化下一轮。
// 运行：java Controller_v3 <controller_v3.conf.json>

import ai_train.*;
import ai_train.Bytes;

import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.*;
import com.zrdds.infrastructure.*;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Controller_v3 {

    // ===== 配置 =====
    private static class Config {
        int domain_id;
        int expected_clients;
        long timeout_ms;
        int rounds;

        String python_exe;
        String eval_script;
        String data_dir;
        int batch_size;
    }
    private static Config config;

    // ===== 话题 =====
    private static final String TOPIC_TRAIN_CMD     = "train/train_cmd";
    private static final String TOPIC_CLIENT_UPDATE = "train/client_update";
    private static final String TOPIC_MODEL_BLOB    = "train/model_blob";

    // ===== 每客户端流式收包器 =====
    private static class ClientStream {
        final List<byte[]> packets = Collections.synchronizedList(new ArrayList<>());
        volatile long numSamples = 0L;       // 仅最后一包>0
        volatile boolean finalReceived = false;
        void addPacket(byte[] data, long ns) {
            packets.add(data);
            if (ns > 0) { finalReceived = true; numSamples = ns; }
        }
    }
    // round -> (client -> stream)
    private static final ConcurrentMap<Integer, ConcurrentMap<Integer, ClientStream>> roundStreams = new ConcurrentHashMap<>();

    // ===== DDS =====
    private DomainParticipant dp;
    private Publisher publisher;
    private Subscriber subscriber;
    private TrainCmdDataWriter  trainCmdWriter;
    private ModelBlobDataWriter modelBlobWriter;
    private ClientUpdateDataReader clientUpdateReader;

    private final AtomicLong roundCounter = new AtomicLong(2);

    // ===== 全局模型（FP32） =====
    private float[] currentModel = null;
    private int     modelDim     = -1;

    // ===== main =====
    public static void main(String[] args) throws Exception {
        if (args.length != 1) { System.err.println("Usage: java Controller_v3 <controller_v3.conf.json>"); return; }
        loadConfig(args[0]);

        Controller_v3 ctrl = new Controller_v3();
        ctrl.init();
        for (int i = 0; i < config.rounds; i++) {
            ctrl.runTrainingRound(60000, 5, 0.01, 12345 + i);
        }
        DDSIF.Finalize();
    }

    private static void loadConfig(String confPath) throws Exception {
        String raw = Files.readString(Path.of(confPath), StandardCharsets.UTF_8);
        JSONObject jo = new JSONObject(raw);
        config = new Config();
        config.domain_id        = jo.getInt("domain_id");
        config.expected_clients = jo.getInt("expected_clients");
        config.timeout_ms       = jo.getLong("timeout_ms");
        config.rounds           = jo.optInt("rounds", 1);
        config.python_exe       = jo.getString("python_exe");
        config.eval_script      = jo.getString("eval_script");
        config.data_dir         = jo.getString("data_dir");
        config.batch_size       = jo.optInt("batch_size", 64);

        System.out.println("[Controller_v3] cfg: domain=" + config.domain_id
                + " expected_clients=" + config.expected_clients
                + " timeout_ms=" + config.timeout_ms
                + " rounds=" + config.rounds);
    }

    private void init() throws InterruptedException {
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        dp = dpf.create_participant(config.domain_id, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (dp == null) throw new RuntimeException("create participant failed");

        TrainCmdTypeSupport.get_instance().register_type(dp, null);
        ClientUpdateTypeSupport.get_instance().register_type(dp, null);
        ModelBlobTypeSupport.get_instance().register_type(dp, null);

        Topic tCmd   = dp.create_topic(TOPIC_TRAIN_CMD,     TrainCmdTypeSupport.get_instance().get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic tUpd   = dp.create_topic(TOPIC_CLIENT_UPDATE, ClientUpdateTypeSupport.get_instance().get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic tModel = dp.create_topic(TOPIC_MODEL_BLOB,    ModelBlobTypeSupport.get_instance().get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        publisher  = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        subscriber = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        DataWriterQos wq = new DataWriterQos();
        publisher.get_default_datawriter_qos(wq);
        wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        wq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        wq.history.depth    = 8;

        trainCmdWriter  = (TrainCmdDataWriter)  publisher.create_datawriter(tCmd,   wq, null, StatusKind.STATUS_MASK_NONE);
        modelBlobWriter = (ModelBlobDataWriter) publisher.create_datawriter(tModel, wq, null, StatusKind.STATUS_MASK_NONE);
        if (trainCmdWriter == null || modelBlobWriter == null) throw new RuntimeException("create writer failed");

        waitWriterMatched(trainCmdWriter, 1, 5000);
        waitWriterMatched(modelBlobWriter, 1, 5000);

        DataReaderQos rq = new DataReaderQos();
        subscriber.get_default_datareader_qos(rq);
        rq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        rq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        rq.history.depth    = 64;

        clientUpdateReader = (ClientUpdateDataReader) subscriber.create_datareader(
                tUpd, rq, new ClientUpdateListener(), StatusKind.DATA_AVAILABLE_STATUS);
        if (clientUpdateReader == null) throw new RuntimeException("create ClientUpdate reader failed");
        waitReaderMatched(clientUpdateReader, 1, 5000);
        System.out.println("[Controller_v3] ready.");
    }

    private static class ClientUpdateListener extends SimpleDataReaderListener<ClientUpdate, ClientUpdateSeq, ClientUpdateDataReader> {
        @Override public void on_data_arrived(DataReader r, Object sample, SampleInfo info) {}
        @Override public void on_process_sample(DataReader r, ClientUpdate cu, SampleInfo info) {
            if (cu == null || info == null || !info.valid_data) return;
            byte[] payload = bytesToArray(cu.data);
            roundStreams.computeIfAbsent(cu.round_id, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(cu.client_id, k -> new ClientStream())
                    .addPacket(payload, cu.num_samples);
            System.out.println("[Controller_v3] recv update: round=" + cu.round_id
                    + " client=" + cu.client_id
                    + " bytes=" + (payload == null ? 0 : payload.length)
                    + " ns=" + cu.num_samples
                    + " magic=" + magicOf(payload));
        }
    }

    public void runTrainingRound(long subsetSize, int epochs, double lr, int seed) throws InterruptedException {
        int roundId = (int) roundCounter.getAndIncrement();
        System.out.println("\n================ Round " + roundId + " ================");
        long t0 = System.currentTimeMillis();

        TrainCmd cmd = new TrainCmd();
        cmd.round_id    = roundId;
        cmd.subset_size = (int) subsetSize;
        cmd.epochs      = epochs;
        cmd.lr          = lr;
        cmd.seed        = seed;
        ReturnCode_t rc = trainCmdWriter.write(cmd, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) { System.err.println("[Controller_v3] write TrainCmd failed: " + rc); return; }
        System.out.println("[Controller_v3] TrainCmd written: round=" + roundId);

        Map<Integer, ClientStream> streams = waitForRoundStreams(roundId, config.expected_clients, config.timeout_ms);
        if (streams.isEmpty()) { System.err.println("[Controller_v3] no updates collected, skip this round."); return; }
        System.out.println("[Controller_v3] collected clients: " + streams.size() + " / expected " + config.expected_clients);

        List<ClientVec> cvs = collectClientVectors(streams);
        applyAndPublish(cvs, roundId);

        long t1 = System.currentTimeMillis();
        System.out.println("[Controller_v3] round time: " + (t1 - t0) + " ms");

        if (currentModel != null) {
            byte[] modelBytes = float32ToBytesLE(currentModel);
            evaluateModel(modelBytes);
        }
        roundStreams.remove(roundId);
    }

    private static Map<Integer, ClientStream> waitForRoundStreams(int roundId, int expectedClients, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        int lastReady = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ConcurrentMap<Integer, ClientStream> m = roundStreams.get(roundId);
            int ready = 0;
            if (m != null) for (ClientStream cs : m.values()) if (cs.finalReceived) ready++;
            if (ready != lastReady) {
                System.out.println("[Controller_v3] progress: final-ready=" + ready + "/" + expectedClients);
                lastReady = ready;
            }
            if (ready >= expectedClients) break;
            Thread.sleep(100);
        }
        ConcurrentMap<Integer, ClientStream> m = roundStreams.get(roundId);
        if (m == null || m.isEmpty()) return Collections.emptyMap();

        Map<Integer, ClientStream> finals = new HashMap<>();
        for (Map.Entry<Integer, ClientStream> e : m.entrySet()) if (e.getValue().finalReceived) finals.put(e.getKey(), e.getValue());
        if (!finals.isEmpty()) return finals;  // 正常路径
        System.err.println("[Controller_v3] WARNING: no final packets, aggregate with partial (may be inexact).");
        return new HashMap<>(m);
    }

    // ===== 收集向量及语义 =====
    private static class ClientVec {
        final float[] v;
        final long    numSamples;
        final boolean isDelta; // true=S4(增量)；false=权重(FP32)
        ClientVec(float[] v, long n, boolean d) { this.v=v; this.numSamples=n; this.isDelta=d; }
    }

    private static List<ClientVec> collectClientVectors(Map<Integer, ClientStream> streams) {
        List<ClientVec> out = new ArrayList<>(streams.size());
        for (Map.Entry<Integer, ClientStream> e : streams.entrySet()) {
            ClientStream cs = e.getValue();
            if (cs.packets.isEmpty()) continue;

            float[] sum = null;
            boolean anyDelta=false, anyWeights=false;
            for (byte[] pkt : cs.packets) {
                float[] v;
                if (isS4Sparse(pkt)) { v = decodeS4SparseToFloat(pkt); anyDelta=true; }
                else { v = bytesToFloat32LE(pkt); anyWeights=true; }  // FP32 权重
                if (sum == null) sum = v.clone();
                else {
                    if (sum.length != v.length) throw new IllegalStateException("dim mismatch among packets");
                    for (int i=0;i<sum.length;i++) sum[i] += v[i];
                }
            }
            boolean isDelta = anyDelta && !anyWeights;
            long n = cs.numSamples > 0 ? cs.numSamples : 1L;
            out.add(new ClientVec(sum, n, isDelta));
        }
        return out;
    }

    // ===== 应用并发布 =====
    // S4 = Δ；FP32 = 权重
    private void applyAndPublish(List<ClientVec> cvs, int roundId) {
        if (cvs == null || cvs.isEmpty()) return;

        final boolean anyWeights = cvs.stream().anyMatch(cv -> !cv.isDelta);
        final boolean allDelta   = cvs.stream().allMatch(cv ->  cv.isDelta);

        float[] result;

        if (anyWeights && !allDelta) {
            // 权重平均（FedAvg）
            int dim = cvs.get(0).v.length;
            for (ClientVec cv : cvs) if (cv.v.length != dim) throw new IllegalStateException("dim mismatch among clients");
            float[] avg = new float[dim];
            double tot = 0.0;
            for (ClientVec cv : cvs) tot += Math.max(1L, cv.numSamples);
            for (ClientVec cv : cvs) {
                float coef = (float)(Math.max(1L, cv.numSamples) / tot);
                for (int i = 0; i < dim; i++) avg[i] += cv.v[i] * coef;
            }
            currentModel = avg;
            modelDim = dim;
            result = currentModel;
        } else if (allDelta) {
            // 增量平均后叠加
            if (currentModel == null) {
                System.err.println("[Controller_v3] WARNING: currentModel is null; bootstrap a weights round first!");
                int dim = cvs.get(0).v.length;
                currentModel = new float[dim];
                modelDim = dim;
            }
            for (ClientVec cv : cvs) if (cv.v.length != modelDim) throw new IllegalStateException("dim mismatch to currentModel");

            float[] delta = new float[modelDim];
            double tot = 0.0;
            for (ClientVec cv : cvs) tot += Math.max(1L, cv.numSamples);
            for (ClientVec cv : cvs) {
                float coef = (float)(Math.max(1L, cv.numSamples) / tot);
                for (int i = 0; i < modelDim; i++) delta[i] += cv.v[i] * coef;
            }
            for (int i = 0; i < modelDim; i++) currentModel[i] += delta[i];
            result = currentModel;
        } else {
            throw new IllegalStateException("Mixed update types not supported in one round");
        }

        byte[] modelBytes = float32ToBytesLE(result);
        ModelBlob blob = new ModelBlob();
        blob.round_id = roundId;
        blob.data = new ai_train.Bytes();
        blob.data.loan_contiguous(modelBytes, modelBytes.length, modelBytes.length);
        ReturnCode_t rc = modelBlobWriter.write(blob, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK)
            System.err.println("[Controller_v3] write ModelBlob failed: " + rc);
        else
            System.out.println("[Controller_v3] published FP32 model, bytes=" + modelBytes.length);
    }

    // ===== 识别/解码 =====
    private static boolean isS4Sparse(byte[] d){ return d!=null && d.length>=4 && d[0]=='S'&&d[1]=='4'&&d[2]==0&&(d[3]&0xFF)==1; }

    private static float[] decodeS4SparseToFloat(byte[] blob) {
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        if (!(bb.get()=='S' && bb.get()=='4' && bb.get()==0 && (bb.get()&0xFF)==1)) throw new IllegalArgumentException("bad S4");
        int dim = bb.getInt(); int k = bb.getInt();
        float[] out = new float[dim];
        int[] idx = new int[k];
        for (int i=0;i<k;i++) idx[i]=bb.getInt();
        for (int i=0;i<k;i++) {
            int id=idx[i]; if (0<=id && id<dim) out[id]+= bb.getFloat();
        }
        return out;
    }

    // ===== FP32 工具 =====
    private static float[] bytesToFloat32LE(byte[] data){
        if (data==null || data.length%4!=0) throw new IllegalArgumentException("fp32 bytes invalid");
        int n=data.length/4; float[] out=new float[n];
        ByteBuffer bb=ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        for(int i=0;i<n;i++) out[i]=bb.getFloat();
        return out;
    }
    private static byte[] float32ToBytesLE(float[] v){
        ByteBuffer bb=ByteBuffer.allocate(v.length*4).order(ByteOrder.LITTLE_ENDIAN);
        for(float f:v) bb.putFloat(f);
        return bb.array();
    }
    private static byte[] bytesToArray(Bytes b){ if(b==null) return new byte[0]; int n=b.length(); byte[] out=new byte[n]; b.to_array(out,n); return out; }
    private static String magicOf(byte[] b){
        if (b==null || b.length<4) return "short";
        int b0=b[0]&0xFF,b1=b[1]&0xFF,b2=b[2]&0xFF,b3=b[3]&0xFF;
        if(b0=='S'&&b1=='4'&&b2==0&&b3==1) return "S4/v1";
        if(b.length%4==0) return "FP32(?)";
        return String.format("??(%02X %02X %02X %02X)",b0,b1,b2,b3);
    }

    // ===== 评估（可选） =====
    private void evaluateModel(byte[] modelData){
        System.out.println("[Controller_v3] Evaluating model, bytes="+modelData.length);
        long t0=System.currentTimeMillis();
        try{
            Path tmp=Files.createTempFile("eval_model_",".bin"); Files.write(tmp, modelData);
            List<String> cmd=Arrays.asList(config.python_exe, config.eval_script,
                    "--model", tmp.toString(), "--data_dir", config.data_dir, "--batch_size", String.valueOf(config.batch_size));
            ProcessBuilder pb=new ProcessBuilder(cmd); pb.redirectErrorStream(true);
            Process p=pb.start();
            try(BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))){
                String line; while((line=br.readLine())!=null) System.out.println("[PY] "+line);
            }
            int exit=p.waitFor(); if(exit!=0) System.err.println("[Controller_v3] Eval exit code: "+exit);
            Files.deleteIfExists(tmp);
        }catch(Exception e){ e.printStackTrace(); }
        System.out.println("[Controller_v3] Eval done.");
    }

    // ===== 匹配等待 =====
    private static boolean waitReaderMatched(DataReader r,int min,long to) throws InterruptedException{
        long s=System.currentTimeMillis(); SubscriptionMatchedStatus st=new SubscriptionMatchedStatus(); int last=-1;
        while(System.currentTimeMillis()-s<to){
            ReturnCode_t rc=r.get_subscription_matched_status(st); if(rc!=ReturnCode_t.RETCODE_OK){ System.err.println("reader status rc="+rc); return false; }
            if(st.current_count!=last){ System.out.println("[Controller_v3] reader matched: current="+st.current_count+" total="+st.total_count); last=st.current_count; }
            if(st.current_count>=min) return true; Thread.sleep(100);
        } return false;
    }
    private static boolean waitWriterMatched(DataWriter w,int min,long to) throws InterruptedException{
        long s=System.currentTimeMillis(); PublicationMatchedStatus st=new PublicationMatchedStatus(); int last=-1;
        while(System.currentTimeMillis()-s<to){
            ReturnCode_t rc=w.get_publication_matched_status(st); if(rc!=ReturnCode_t.RETCODE_OK){ System.err.println("writer status rc="+rc); return false; }
            if(st.current_count!=last){ System.out.println("[Controller_v3] writer matched: current="+st.current_count+" total="+st.total_count); last=st.current_count; }
            if(st.current_count>=min) return true; Thread.sleep(100);
        } return false;
    }
}
