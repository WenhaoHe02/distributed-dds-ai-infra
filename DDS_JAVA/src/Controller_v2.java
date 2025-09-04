import ai_train.Bytes;
import com.zrdds.infrastructure.*;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.*;
import com.zrdds.subscription.*;
import com.zrdds.publication.*;
import com.zrdds.topic.*;
import com.zrdds.infrastructure.ReliabilityQosPolicyKind;
import com.zrdds.infrastructure.HistoryQosPolicyKind;
import com.zrdds.publication.DataWriterQos;
import ai_train.*;

import org.json.JSONObject;

import java.io.BufferedReader;
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
 * Controller_v2: 支持 INT8 稀疏(sparse)与 dense(Q8) 以及 FP32 回退的聚合
 * 运行：java Controller_v2 <controller_v2.conf.json>
 *
 * 稀疏 INT8 二进制格式（小端）：
 *   magic: 'S','8',0, ver(uint8)=1
 *   dim(int32)
 *   k(int32)
 *   scale(float32)
 *   indices[int32 * k]
 *   values[int8  * k]
 *
 * 兼容 dense INT8 与 FP32：
 * - Q8 dense: 'Q','8',0, ver=1, chunk(int32), total_len(int64), nChunks(int32), scales(float[nChunks]), qvals(int8[...])
 * - FP32 bytes: 4 * dim
 */
public class Controller_v2 {

    private static class Config {
        int domain_id;
        int expected_clients;
        long timeout_ms;
        int rounds;

        // 评估配置
        String python_exe;
        String eval_script;
        String data_dir;
        int batch_size;
    }

    private static Config config;

    private static final String TOPIC_TRAIN_CMD     = "train/train_cmd";
    private static final String TOPIC_CLIENT_UPDATE = "train/client_update";
    private static final String TOPIC_MODEL_BLOB    = "train/model_blob";

    private static final ConcurrentMap<Integer, List<ClientUpdate>> updatesMap = new ConcurrentHashMap<>();

    private DomainParticipant dp;
    private Publisher publisher;
    private Subscriber subscriber;
    private DataWriter trainCmdWriter;
    private DataWriter modelBlobWriter;

    private DataReader clientUpdateReader;

    private AtomicLong roundCounter = new AtomicLong(1);

    private static class ClientUpdateListener extends SimpleDataReaderListener<ClientUpdate, ClientUpdateSeq, ClientUpdateDataReader> {
        @Override
        public void on_process_sample(DataReader reader, ClientUpdate cu, SampleInfo info) {
            if (cu == null || info == null || !info.valid_data) return;
            updatesMap.computeIfAbsent(cu.round_id, k -> Collections.synchronizedList(new ArrayList<>())).add(cu);
            System.out.println("[Controller_v2] recv update: client=" + cu.client_id
                    + " round=" + cu.round_id
                    + " bytes=" + (cu.data == null ? 0 : cu.data.length())
                    + " nsamples=" + cu.num_samples);
        }
        @Override public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {}
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java Controller_v2 <controller_v2.conf.json>");
            return;
        }
        loadConfig(args[0]);

        Controller_v2 ctrl = new Controller_v2();
        ctrl.init();
        int rounds = config.rounds;
        for (int i = 0; i < rounds; i++) {
            ctrl.runTrainingRound(3000, 5, 0.01, 12345 + i);
        }

        DDSIF.Finalize();
    }

    private static void loadConfig(String confPath) throws Exception {
        String raw = Files.readString(Paths.get(confPath), StandardCharsets.UTF_8);
        JSONObject jo = new JSONObject(raw);
        config = new Config();
        config.domain_id        = jo.getInt("domain_id");
        config.expected_clients = jo.getInt("expected_clients");
        config.timeout_ms       = jo.getLong("timeout_ms");
        config.python_exe  = jo.getString("python_exe");
        config.eval_script = jo.getString("eval_script");
        config.data_dir    = jo.getString("data_dir");
        config.batch_size  = jo.optInt("batch_size", 64);
        config.rounds      = jo.optInt("rounds", 1);
        System.out.println("[Controller_v2] cfg: domain=" + config.domain_id
                + " expected_clients=" + config.expected_clients
                + " timeout_ms=" + config.timeout_ms);
    }

    /** 等待 DataReader 至少匹配到 minMatches 个 DataWriter，并打印状态 */
    private static boolean waitReaderMatched(DataReader reader, int minMatches, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        SubscriptionMatchedStatus st = new SubscriptionMatchedStatus();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ReturnCode_t rc = reader.get_subscription_matched_status(st);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Controller_v2] get_subscription_matched_status rc=" + rc);
                return false;
            }
            if (st.current_count != last) {
                System.out.println("[Controller_v2] Reader matched: current=" + st.current_count
                        + " total=" + st.total_count
                        + " change=" + st.current_count_change);
                last = st.current_count;
            }
            if (st.current_count >= minMatches) return true;
            Thread.sleep(100);
        }
        return false;
    }

    /** 等待 DataWriter 至少匹配到 minMatches 个 DataReader，并打印状态 */
    private static boolean waitWriterMatched(DataWriter writer, int minMatches, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        PublicationMatchedStatus st = new PublicationMatchedStatus();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            ReturnCode_t rc = writer.get_publication_matched_status(st);
            if (rc != ReturnCode_t.RETCODE_OK) {
                System.err.println("[Controller_v2] get_publication_matched_status rc=" + rc);
                return false;
            }
            if (st.current_count != last) {
                System.out.println("[Controller_v2] Writer matched: current=" + st.current_count
                        + " total=" + st.total_count
                        + " change=" + st.current_count_change);
                last = st.current_count;
            }
            if (st.current_count >= minMatches) return true;
            Thread.sleep(100);
        }
        return false;
    }

    private void init() throws InterruptedException {
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        dp = dpf.create_participant(config.domain_id, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (dp == null) throw new RuntimeException("create participant failed");

        TrainCmdTypeSupport.get_instance().register_type(dp, null);
        ClientUpdateTypeSupport.get_instance().register_type(dp, null);
        ModelBlobTypeSupport.get_instance().register_type(dp, null);

        Topic trainCmdTopic = dp.create_topic(TOPIC_TRAIN_CMD, TrainCmdTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic clientUpdateTopic = dp.create_topic(TOPIC_CLIENT_UPDATE, ClientUpdateTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic modelBlobTopic = dp.create_topic(TOPIC_MODEL_BLOB, ModelBlobTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        publisher  = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        subscriber = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        DataWriterQos wq = new DataWriterQos();
        publisher.get_default_datawriter_qos(wq);
        wq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        wq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        wq.history.depth    = 2;

        // Writers
        trainCmdWriter  = publisher.create_datawriter(trainCmdTopic, wq, null, StatusKind.STATUS_MASK_NONE);
        if (trainCmdWriter == null) throw new RuntimeException("create TrainCmd writer failed");
        System.out.println("[Controller_v2] TrainCmd writer created");
        boolean w1 = waitWriterMatched(trainCmdWriter, 1, 5000);
        System.out.println("[Controller_v2] TrainCmd writer matched=" + w1);

        modelBlobWriter = publisher.create_datawriter(modelBlobTopic, wq, null, StatusKind.STATUS_MASK_NONE);
        if (modelBlobWriter == null) throw new RuntimeException("create ModelBlob writer failed");
        System.out.println("[Controller_v2] ModelBlob writer created");
        boolean w2 = waitWriterMatched(modelBlobWriter, 1, 5000);
        System.out.println("[Controller_v2] ModelBlob writer matched=" + w2);

        DataReaderQos rq = new DataReaderQos();
        subscriber.get_default_datareader_qos(rq);
        rq.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        rq.history.kind     = HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS;
        rq.history.depth    = 2;

        // Reader
        clientUpdateReader = subscriber.create_datareader(
                clientUpdateTopic,
                rq,
                new ClientUpdateListener(),
                StatusKind.DATA_AVAILABLE_STATUS
        );
        if (clientUpdateReader == null) throw new RuntimeException("create ClientUpdate reader failed");
        System.out.println("[Controller_v2] ClientUpdate reader created");
        boolean r1 = waitReaderMatched(clientUpdateReader, 1, 5000);
        System.out.println("[Controller_v2] ClientUpdate reader matched=" + r1);
    }

    // 发起训练一轮
    public void runTrainingRound(long subsetSize, int epochs, double lr, int seed) throws InterruptedException {
        int roundId = (int) roundCounter.getAndIncrement();
        System.out.println("[Controller_v2] start round " + roundId);
        long tStart = System.currentTimeMillis();

        // 再次确认 TrainCmd writer 已有订阅者（跨机时发现可能更慢）
        boolean ready = waitWriterMatched(trainCmdWriter, 1, 3000);
        System.out.println("[Controller_v2] Before send TrainCmd matched=" + ready);

        TrainCmd cmd = new TrainCmd();
        cmd.round_id    = roundId;
        cmd.subset_size = (int) subsetSize;
        cmd.epochs      = epochs;
        cmd.lr          = lr;
        cmd.seed        = seed;

        ReturnCode_t rc = ((TrainCmdDataWriter)trainCmdWriter).write(cmd, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller_v2] write TrainCmd failed: " + rc);
            return;
        } else {
            System.out.println("[Controller_v2] TrainCmd written OK (round=" + roundId + ")");
        }


        List<ClientUpdate> collected = waitForClientUpdates(roundId, config.expected_clients, config.timeout_ms);
        if (collected.isEmpty()) {
            System.err.println("[Controller_v2] no updates collected, skipping aggregation for this round");
            return;
        }
        System.out.println("[Controller_v2] collected " + collected.size() + " updates (expected " + config.expected_clients + ")");


        // 聚合（自动识别 S8 / Q8 / FP32）
        float[] aggregated = aggregateFedAvgAuto(collected);
        byte[]  modelData  = float32ToBytesLE(aggregated);

        // 发布聚合模型（FP32）
        ModelBlob blob = new ModelBlob();
        blob.round_id = roundId;
        blob.data = new Bytes();
        blob.data.loan_contiguous(modelData, modelData.length, modelData.length);
        rc = ((ModelBlobDataWriter)modelBlobWriter).write(blob, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[Controller_v2] write ModelBlob failed: " + rc);
        } else {
            System.out.println("[Controller_v2] published model, bytes=" + modelData.length);
        }

        long tEnd = System.currentTimeMillis();
        System.out.println("[Controller_v2] e2e time: " + (tEnd - tStart) + " ms");

        // 评估
        evaluateModel(modelData);

        updatesMap.remove(roundId);
    }

    private List<ClientUpdate> waitForClientUpdates(int roundId, int expectedClients, long timeoutMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        int last = -1;
        while (System.currentTimeMillis() - start < timeoutMs) {
            List<ClientUpdate> list = updatesMap.get(roundId);
            int cnt = (list == null ? 0 : list.size());
            if (cnt != last) {
                System.out.println("[Controller_v2] progress " + cnt + "/" + expectedClients);
                last = cnt;
            }
            if (cnt >= expectedClients) break;
            Thread.sleep(100);
        }
        List<ClientUpdate> list = updatesMap.get(roundId);
        if (list == null || list.isEmpty()) {
            System.err.println("[Controller_v2] WARNING: No updates received within timeout " + timeoutMs + " ms");
            return Collections.emptyList();
        }
        if (list.size() < expectedClients) {
            System.err.println("[Controller_v2] WARNING: Timeout reached (" + timeoutMs + " ms), only "
                    + list.size() + "/" + expectedClients + " clients responded. Proceeding with partial aggregation.");
        }
        return new ArrayList<>(list);
    }



    // 自动识别 + 解码（S8/Q8/FP32）
    private static float[] aggregateFedAvgAuto(List<ClientUpdate> updates) {
        if (updates.isEmpty()) return new float[0];

        List<float[]> vecs = new ArrayList<>(updates.size());
        List<Long>    ws   = new ArrayList<>(updates.size());
        int dim = -1;

        for (ClientUpdate cu : updates) {
            byte[] payload = new byte[cu.data.length()];
            for (int i = 0; i < payload.length; i++) payload[i] = cu.data.get_at(i);

            float[] v;
            if (isS8Sparse(payload)) {
                v = decodeS8SparseToFloat(payload);
            } else if (isQ8Dense(payload)) {
                v = decodeQ8ToFloat(payload);
            } else {
                v = bytesToFloat32LE(payload);
            }
            if (dim < 0) dim = v.length;
            if (v.length != dim) throw new IllegalStateException("inconsistent dim among clients");
            vecs.add(v);
            ws.add(cu.num_samples);
        }

        double totalW = 0.0;
        for (Long w : ws) totalW += Math.max(0L, w);
        if (totalW <= 0) totalW = updates.size();

        float[] out = new float[dim];
        Arrays.fill(out, 0f);

        for (int i = 0; i < vecs.size(); i++) {
            float[] v = vecs.get(i);
            double  w = (ws.get(i) > 0 ? ws.get(i) : 1);
            float   coef = (float)(w / totalW);
            for (int j = 0; j < dim; j++) out[j] += v[j] * coef;
        }
        return out;
    }

    // ====== 识别/解码：S8 稀疏 ======
    private static boolean isS8Sparse(byte[] data) {
        return data.length >= 4 && data[0]=='S' && data[1]=='8' && data[2]==0 && (data[3]&0xFF)==1;
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
            if (id < 0 || id >= dim) continue; // 容错
            out[id] += q * scale;
        }
        return out;
    }

    // ====== 识别/解码：Q8 稠密 ======
    private static boolean isQ8Dense(byte[] data) {
        return data.length >= 4 && data[0]=='Q' && data[1]=='8' && data[2]==0 && (data[3]&0xFF)==1;
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

    // ====== FP32 小端工具 ======
    private static float[] bytesToFloat32LE(byte[] data) {
        if (data.length % 4 != 0) throw new IllegalArgumentException("fp32 bytes length not multiple of 4");
        int n = data.length / 4;
        float[] out = new float[n];
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        for (int i=0;i<n;i++) out[i] = bb.getFloat();
        return out;
    }
    private static byte[] float32ToBytesLE(float[] v) {
        ByteBuffer bb = ByteBuffer.allocate(v.length*4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f: v) bb.putFloat(f);
        return bb.array();
    }

    // 调用 Python 评估（保持 FP32）
    private void evaluateModel(byte[] modelData) {
        System.out.println("[Controller_v2] Evaluating model, bytes=" + modelData.length);
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

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println("[PY] " + line);
                    sb.append(line).append('\n');
                }
            }
            int exit = p.waitFor();
            if (exit != 0) {
                System.err.println("[Controller_v2] Eval exit code: " + exit);
                Files.deleteIfExists(tmpModel);
                return;
            }

            double acc = parseAccuracy(sb.toString());
            long t1 = System.currentTimeMillis();
            System.out.printf("[Controller_v2] Eval done: Accuracy=%.4f, Time=%d ms%n", acc, (t1 - t0));
            Files.deleteIfExists(tmpModel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static double parseAccuracy(String output) {
        try {
            int i = output.indexOf("Accuracy:");
            if (i >= 0) {
                int j = i + "Accuracy:".length();
                while (j < output.length() && Character.isWhitespace(output.charAt(j))) j++;
                int k = j;
                while (k < output.length() && (Character.isDigit(output.charAt(k)) || output.charAt(k)=='.')) k++;
                return Double.parseDouble(output.substring(j, k));
            }
        } catch (Exception ignore) {}
        return -1.0;
    }
}
