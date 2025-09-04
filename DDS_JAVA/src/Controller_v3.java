// Controller_v3.java
// 聚合端：支持多包（流式）与单包；自动识别 S8/Q8/FP32；
// 语义：S8=“模型增量 Δ”，对同一客户端多包先相加→做 FedAvg→叠加到 currentModel；
//      Q8/FP32=“完整权重”，直接做权重平均→作为新 currentModel。
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

    // =============== 配置 ===============
    private static class Config {
        int domain_id;
        int expected_clients;
        long timeout_ms;
        int rounds;

        // 评估
        String python_exe;
        String eval_script;
        String data_dir;
        int batch_size;
    }
    private static Config config;

    // =============== 话题名 ===============
    private static final String TOPIC_TRAIN_CMD     = "train/train_cmd";
    private static final String TOPIC_CLIENT_UPDATE = "train/client_update";
    private static final String TOPIC_MODEL_BLOB    = "train/model_blob";

    // =============== 每客户端流式收包聚合器 ===============
    private static class ClientStream {
        final List<byte[]> packets = Collections.synchronizedList(new ArrayList<>());
        volatile long numSamples = 0L;       // 仅“最后一包”携带；其余为 0
        volatile boolean finalReceived = false;
        volatile long lastTs = System.currentTimeMillis();

        void addPacket(byte[] data, long ns) {
            packets.add(data);
            if (ns > 0) { finalReceived = true; numSamples = ns; }
            lastTs = System.currentTimeMillis();
        }
    }

    // round_id -> (client_id -> ClientStream)
    private static final ConcurrentMap<Integer, ConcurrentMap<Integer, ClientStream>> roundStreams = new ConcurrentHashMap<>();

    // =============== DDS 成员 ===============
    private DomainParticipant dp;
    private Publisher publisher;
    private Subscriber subscriber;
    private TrainCmdDataWriter trainCmdWriter;
    private ModelBlobDataWriter modelBlobWriter;
    private ClientUpdateDataReader clientUpdateReader;

    private final AtomicLong roundCounter = new AtomicLong(1);

    // =============== 全局模型（FP32） ===============
    private float[] currentModel = null;
    private int     modelDim     = -1;

    // =============== 入口 ===============
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java Controller_v3 <controller_v3.conf.json>");
            return;
        }
        loadConfig(args[0]);

        Controller_v3 ctrl = new Controller_v3();
        ctrl.init();
        int rounds = config.rounds;
        for (int i = 0; i < rounds; i++) {
            ctrl.runTrainingRound(3000, 5, 0.01, 12345 + i);
        }
        DDSIF.Finalize();
    }

    // =============== 配置加载 ===============
    private static void loadConfig(String confPath) throws Exception {
        String raw = Files.readString(Path.of(confPath), StandardCharsets.UTF_8);
        JSONObject jo = new JSONObject(raw);
        Config c = new Config();
        c.domain_id        = jo.getInt("domain_id");
        c.expected_clients = jo.getInt("expected_clients");
        c.timeout_ms       = jo.getLong("timeout_ms");
        c.python_exe       = jo.getString("python_exe");
        c.eval_script      = jo.getString("eval_script");
        c.data_dir         = jo.getString("data_dir");
        c.batch_size       = jo.optInt("batch_size", 64);
        c.rounds           = jo.optInt("rounds", 1);
        config = c;

        System.out.println("[Controller_v3] cfg: domain=" + c.domain_id
                + " expected_clients=" + c.expected_clients
                + " timeout_ms=" + c.timeout_ms
                + " rounds=" + c.rounds);
    }

    // =============== 初始化 DDS ===============
    private void init() throws InterruptedException {
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        dp = dpf.create_participant(config.domain_id, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
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

        DataWriterQos wq = new DataWriterQos();
        publisher.get_default_datawriter_qos(wq);
        wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        wq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        wq.history.depth    = 8; // 多包更稳

        trainCmdWriter  = (TrainCmdDataWriter) publisher.create_datawriter(tCmd, wq, null, StatusKind.STATUS_MASK_NONE);
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

    // =============== Listener：收包入库（支持多包） ===============
    private static class ClientUpdateListener extends SimpleDataReaderListener<ClientUpdate, ClientUpdateSeq, ClientUpdateDataReader> {
        @Override public void on_data_arrived(DataReader r, Object sample, SampleInfo info) {}

        @Override
        public void on_process_sample(DataReader reader, ClientUpdate cu, SampleInfo info) {
            if (cu == null || info == null || !info.valid_data) return;
            int roundId  = cu.round_id;
            int clientId = cu.client_id;
            long ns      = cu.num_samples;

            // Bytes -> byte[]
            byte[] payload = bytesToArray(cu.data);
            String magic = magicOf(payload);

            // 放入 roundStreams
            roundStreams.computeIfAbsent(roundId, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(clientId, k -> new ClientStream())
                    .addPacket(payload, ns);

            System.out.println("[Controller_v3] recv update: round=" + roundId
                    + " client=" + clientId
                    + " bytes=" + (payload == null ? 0 : payload.length)
                    + " ns=" + ns
                    + " magic=" + magic);
        }
    }

    // =============== 训练一轮 ===============
    public void runTrainingRound(long subsetSize, int epochs, double lr, int seed) throws InterruptedException {
        int roundId = (int) roundCounter.getAndIncrement();
        System.out.println("\n================ Round " + roundId + " ================");
        long t0 = System.currentTimeMillis();

        // 发送 TrainCmd
        TrainCmd cmd = new TrainCmd();
        cmd.round_id    = roundId;
        cmd.subset_size = (int) subsetSize;
        cmd.epochs      = epochs;
        cmd.lr          = lr;
        cmd.seed        = seed;
        ReturnCode_t rc = trainCmdWriter.write(cmd, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller_v3] write TrainCmd failed: " + rc);
            return;
        }
        System.out.println("[Controller_v3] TrainCmd written: round=" + roundId);

        // 等待汇集（直到 expected_clients 个客户端都到齐“最终包”或超时）
        Map<Integer, ClientStream> streams = waitForRoundStreams(roundId, config.expected_clients, config.timeout_ms);
        if (streams.isEmpty()) {
            System.err.println("[Controller_v3] no updates collected, skip this round.");
            return;
        }
        System.out.println("[Controller_v3] collected clients: " + streams.size() + " / expected " + config.expected_clients);

        // 收集向量及其语义
        List<ClientVec> cvs = collectClientVectors(streams);

        // 应用到全局并发布
        applyAndPublish(cvs, roundId);

        long t1 = System.currentTimeMillis();
        System.out.println("[Controller_v3] round time: " + (t1 - t0) + " ms");

        // 可选评估：对 currentModel 评估
        if (currentModel != null) {
            byte[] modelData = float32ToBytesLE(currentModel);
            evaluateModel(modelData);
        }

        // 清理这一轮缓存
        roundStreams.remove(roundId);
    }

    // 等待一轮内各客户端的“最终包”
    private static Map<Integer, ClientStream> waitForRoundStreams(int roundId, int expectedClients, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        int lastReady = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ConcurrentMap<Integer, ClientStream> m = roundStreams.get(roundId);
            int have = (m == null ? 0 : m.size());
            int ready = 0;
            if (m != null) for (ClientStream cs : m.values()) if (cs.finalReceived) ready++;
            if (ready != lastReady) {
                System.out.println("[Controller_v3] progress: clients=" + have + ", final-ready=" + ready + "/" + expectedClients);
                lastReady = ready;
            }
            if (ready >= expectedClients) break;
            Thread.sleep(100);
        }
        ConcurrentMap<Integer, ClientStream> m = roundStreams.get(roundId);
        if (m == null || m.isEmpty()) return Collections.emptyMap();

        Map<Integer, ClientStream> finals = new HashMap<>();
        for (Map.Entry<Integer, ClientStream> e : m.entrySet()) {
            if (e.getValue().finalReceived) finals.put(e.getKey(), e.getValue());
        }
        if (finals.size() >= Math.min(expectedClients, m.size())) return finals;

        System.err.println("[Controller_v3] WARNING: timeout; only " + finals.size() + " clients have final packets. Falling back to partial aggregation.");
        return new HashMap<>(m);
    }

    // =============== 收集每客户端向量及语义 ===============
    private static class ClientVec {
        final float[] v;
        final long    numSamples;
        final boolean isDelta; // true=S8(增量); false=Q8/FP32(权重)
        ClientVec(float[] v, long n, boolean d) { this.v=v; this.numSamples=n; this.isDelta=d; }
    }

    private static List<ClientVec> collectClientVectors(Map<Integer, ClientStream> streams) {
        List<ClientVec> out = new ArrayList<>(streams.size());
        for (Map.Entry<Integer, ClientStream> e : streams.entrySet()) {
            ClientStream cs = e.getValue();
            if (cs.packets.isEmpty()) continue;

            float[] sum = null;
            boolean anyDelta = false, anyWeights = false;

            for (byte[] pkt : cs.packets) {
                float[] v;
                if (isS8Sparse(pkt)) { v = decodeS8SparseToFloat(pkt); anyDelta = true; }
                else if (isQ8Dense(pkt)) { v = decodeQ8ToFloat(pkt); anyWeights = true; }
                else { v = bytesToFloat32LE(pkt); anyWeights = true; }

                if (sum == null) sum = v.clone();
                else {
                    if (sum.length != v.length) throw new IllegalStateException("inconsistent dim among packets");
                    for (int i=0;i<sum.length;i++) sum[i] += v[i]; // 将多包相加
                }
            }
            boolean isDelta = anyDelta && !anyWeights; // 只要出现非S8就按“权重”
            long n = cs.numSamples > 0 ? cs.numSamples : 1L;
            out.add(new ClientVec(sum, n, isDelta));
        }
        return out;
    }

    // =============== 应用到全局并发布 ===============
    private void applyAndPublish(List<ClientVec> cvs, int roundId) {
        if (cvs.isEmpty()) return;

        boolean anyWeights = cvs.stream().anyMatch(cv -> !cv.isDelta);
        boolean allDelta   = cvs.stream().allMatch(cv -> cv.isDelta);

        float[] result;

        if (anyWeights && !allDelta) {
            // 作为“完整权重”做 FedAvg
            int dim = cvs.get(0).v.length;
            result = new float[dim];
            double tot = 0.0; for (ClientVec cv: cvs) tot += Math.max(1L, cv.numSamples);
            for (ClientVec cv: cvs) {
                if (cv.v.length != dim) throw new IllegalStateException("dim mismatch");
                float coef = (float)(Math.max(1L, cv.numSamples) / tot);
                for (int i=0;i<dim;i++) result[i] += cv.v[i] * coef;
            }
            currentModel = result;
            modelDim     = dim;
        } else if (allDelta) {
            if (currentModel == null) {
                System.err.println("[Controller_v3] WARNING: currentModel is null; bootstrap a weights round first!");
                int dim = cvs.get(0).v.length;
                currentModel = new float[dim]; // 从零起步（可行但效果差）
                modelDim = dim;
            }
            if (modelDim != cvs.get(0).v.length) throw new IllegalStateException("dim mismatch to currentModel");

            // FedAvg 得到 Δ，再叠加
            float[] delta = new float[modelDim];
            double tot = 0.0; for (ClientVec cv: cvs) tot += Math.max(1L, cv.numSamples);
            for (ClientVec cv: cvs) {
                float coef = (float)(Math.max(1L, cv.numSamples) / tot);
                for (int i=0;i<modelDim;i++) delta[i] += cv.v[i] * coef;
            }
            for (int i=0;i<modelDim;i++) currentModel[i] += delta[i];
            result = currentModel;
        } else {
            return; // 不会到这
        }

        byte[] modelBytes = float32ToBytesLE(result);
        ModelBlob blob = new ModelBlob();
        blob.round_id = roundId;
        blob.data = new ai_train.Bytes();
        blob.data.loan_contiguous(modelBytes, modelBytes.length, modelBytes.length);
        ReturnCode_t rc = modelBlobWriter.write(blob, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller_v3] write ModelBlob failed: " + rc);
        } else {
            System.out.println("[Controller_v3] published FP32 model, bytes=" + modelBytes.length);
        }
    }

    // =============== 识别/解码：S8 稀疏 ===============
    private static boolean isS8Sparse(byte[] data) {
        return data != null && data.length >= 4 && data[0]=='S' && data[1]=='8' && data[2]==0 && (data[3] & 0xFF) == 1;
    }
    private static float[] decodeS8SparseToFloat(byte[] blob) {
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        byte m0=bb.get(), m1=bb.get(), m2=bb.get(); int ver = bb.get() & 0xFF;
        if (m0!='S' || m1!='8' || m2!=0 || ver!=1) throw new IllegalArgumentException("bad S8 header");
        int dim = bb.getInt();
        int k   = bb.getInt();
        float scale = bb.getFloat();
        float[] out = new float[dim];
        int[] idx = new int[k];
        for (int i=0;i<k;i++) idx[i] = bb.getInt();
        for (int i=0;i<k;i++) {
            byte q = bb.get();
            int id = idx[i];
            if (id >= 0 && id < dim) out[id] += q * scale;
        }
        return out;
    }

    // =============== 识别/解码：Q8 稠密 ===============
    private static boolean isQ8Dense(byte[] data) {
        return data != null && data.length >= 4 && data[0]=='Q' && data[1]=='8' && data[2]==0 && (data[3] & 0xFF) == 1;
    }
    private static float[] decodeQ8ToFloat(byte[] blob) {
        ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
        byte m0=bb.get(), m1=bb.get(), m2=bb.get(); int ver = bb.get() & 0xFF;
        if (m0!='Q' || m1!='8' || m2!=0 || ver!=1) throw new IllegalArgumentException("bad Q8 header");
        int chunk = bb.getInt();
        long totalL = bb.getLong();
        int nChunks = bb.getInt();
        if (totalL > Integer.MAX_VALUE) throw new IllegalArgumentException("vector too large");
        int total = (int) totalL;

        float[] scales = new float[nChunks];
        for (int i=0;i<nChunks;i++) scales[i] = bb.getFloat();

        float[] out = new float[total];
        for (int ci=0; ci<nChunks; ci++) {
            int s = ci*chunk;
            int e = Math.min(s+chunk, total);
            float sc = scales[ci];
            for (int j=s; j<e; j++) {
                byte q = bb.get();
                out[j] = q * sc;
            }
        }
        return out;
    }

    // =============== FP32 工具&杂项 ===============
    private static float[] bytesToFloat32LE(byte[] data) {
        if (data == null || data.length % 4 != 0) throw new IllegalArgumentException("fp32 bytes length invalid");
        int n = data.length / 4;
        float[] out = new float[n];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        for (int i=0;i<n;i++) out[i] = bb.getFloat();
        return out;
    }
    private static byte[] float32ToBytesLE(float[] v) {
        ByteBuffer bb = ByteBuffer.allocate(v.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f: v) bb.putFloat(f);
        return bb.array();
    }
    private static byte[] bytesToArray(Bytes b) {
        if (b == null) return new byte[0];
        int n = b.length();
        byte[] out = new byte[n];
        b.to_array(out, n);
        return out;
    }
    private static String magicOf(byte[] b) {
        if (b == null || b.length < 4) return "short";
        int b0 = b[0] & 0xFF, b1 = b[1] & 0xFF, b2 = b[2] & 0xFF, b3 = b[3] & 0xFF;
        if (b0=='S' && b1=='8' && b2==0 && b3==1) return "S8/v1";
        if (b0=='Q' && b1=='8' && b2==0 && b3==1) return "Q8/v1";
        if (b.length % 4 == 0) return "FP32(?)";
        return String.format("??(%02X %02X %02X %02X)", b0,b1,b2,b3);
    }

    // =============== 评估 ===============
    private void evaluateModel(byte[] modelData) {
        System.out.println("[Controller_v3] Evaluating model, bytes=" + modelData.length);
        long t0 = System.currentTimeMillis();
        try {
            Path tmpModel = Files.createTempFile("eval_model_", ".bin");
            Files.write(tmpModel, modelData);

            List<String> cmd = new ArrayList<>();
            cmd.add(config.python_exe);
            cmd.add(config.eval_script);
            cmd.add("--model");      cmd.add(tmpModel.toString());
            cmd.add("--data_dir");   cmd.add(config.data_dir);
            cmd.add("--batch_size"); cmd.add(String.valueOf(config.batch_size));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();

            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println("[PY] " + line);
                }
            }
            int exit = p.waitFor();
            if (exit != 0) System.err.println("[Controller_v3] Eval exit code: " + exit);
            Files.deleteIfExists(tmpModel);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long t1 = System.currentTimeMillis();
        System.out.println("[Controller_v3] Eval done. time=" + (t1 - t0) + " ms");
    }

    // =============== DDS 匹配等待 ===============
    private static boolean waitReaderMatched(DataReader reader, int minMatches, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        SubscriptionMatchedStatus st = new SubscriptionMatchedStatus();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ReturnCode_t rc = reader.get_subscription_matched_status(st);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Controller_v3] get_subscription_matched_status rc=" + rc);
                return false;
            }
            if (st.current_count != last) {
                System.out.println("[Controller_v3] reader matched: current=" + st.current_count
                        + " total=" + st.total_count + " change=" + st.current_count_change);
                last = st.current_count;
            }
            if (st.current_count >= minMatches) return true;
            Thread.sleep(100);
        }
        return false;
    }
    private static boolean waitWriterMatched(DataWriter writer, int minMatches, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        PublicationMatchedStatus st = new PublicationMatchedStatus();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ReturnCode_t rc = writer.get_publication_matched_status(st);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Controller_v3] get_publication_matched_status rc=" + rc);
                return false;
            }
            if (st.current_count != last) {
                System.out.println("[Controller_v3] writer matched: current=" + st.current_count
                        + " total=" + st.total_count + " change=" + st.current_count_change);
                last = st.current_count;
            }
            if (st.current_count >= minMatches) return true;
            Thread.sleep(100);
        }
        return false;
    }
}
