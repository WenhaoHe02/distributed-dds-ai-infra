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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class Client {
    private int domainId;
    private int clientId;
    private int numClients;
    private int batchSize;
    private String pythonExe;
    private String trainerPy;
    private String dataDir;

    private DomainParticipant dp;
    private Publisher pub;
    private Subscriber sub;
    private Topic tCmd, tUpd, tModel;

    private TrainCmdDataReader cmdReader;
    private ModelBlobDataReader modelReader; // 可选
    private ClientUpdateDataWriter updWriter;

    private volatile int lastRound = -1;

    public Client(String configPath) throws Exception {
        loadConfig(configPath);
    }

    private void loadConfig(String path) throws Exception {
        String jsonText = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        JSONObject j = new JSONObject(jsonText);
        this.domainId   = j.getInt("domain_id");
        this.clientId   = j.getInt("client_id");
        this.numClients = j.getInt("num_clients");
        this.batchSize  = j.getInt("batch_size");
        this.pythonExe  = j.getString("python_exe");
        this.trainerPy  = j.getString("trainer_py");
        this.dataDir    = j.getString("data_dir");
    }

    public void start() {
        dp = DomainParticipantFactory.get_instance().create_participant(
                domainId, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        TrainCmdTypeSupport.get_instance().register_type(dp, null);
        ClientUpdateTypeSupport.get_instance().register_type(dp, null);
        ModelBlobTypeSupport.get_instance().register_type(dp, null);

        tCmd = dp.create_topic("train/train_cmd", TrainCmdTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        tUpd = dp.create_topic("train/client_update", ClientUpdateTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        tModel = dp.create_topic("train/model_blob", ModelBlobTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        updWriter = (ClientUpdateDataWriter) pub.create_datawriter(tUpd, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        cmdReader = (TrainCmdDataReader) sub.create_datareader(tCmd, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        modelReader = (ModelBlobDataReader) sub.create_datareader(tModel, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        cmdReader.set_listener(new SimpleDataReaderListener<TrainCmd, TrainCmdSeq, TrainCmdDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}
            @Override public void on_process_sample(DataReader r, TrainCmd cmd, SampleInfo info) {
                if (cmd == null || info == null || !info.valid_data) return;
                int round = (int) cmd.round_id;
                if (round <= lastRound) return;
                lastRound = round;

                System.out.println("[JavaDDS] TrainCmd: round=" + round);
                try {
                    TrainResult tr = runPythonTraining(clientId, cmd.seed, cmd.subset_size, cmd.epochs, cmd.lr);
                    ClientUpdate upd = new ClientUpdate();
                    upd.client_id = clientId;
                    upd.round_id = cmd.round_id;
                    upd.num_samples = (long) tr.numSamples;
                    upd.data = toBytes(tr.bytes);
                    updWriter.write(upd, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    System.out.println("[JavaDDS] sent ClientUpdate: round=" + round);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        modelReader.set_listener(new SimpleDataReaderListener<ModelBlob, ModelBlobSeq, ModelBlobDataReader>() {
            @Override public void on_data_arrived(DataReader r, Object o, SampleInfo info) {}
            @Override public void on_process_sample(DataReader r, ModelBlob mb, SampleInfo info) {
                if (mb == null || info == null || !info.valid_data) return;
                System.out.println("[JavaDDS] ModelBlob: round=" + mb.round_id + " bytes=" + bytesLen(mb.data));
            }
        }, StatusKind.DATA_AVAILABLE_STATUS);

        System.out.println("[JavaDDS] client started. Waiting for TrainCmd...");
    }

    private TrainResult runPythonTraining(int clientId, int seed, int subset, int epochs, double lr) throws Exception {
        Path outBin = Files.createTempFile("upd_", ".bin");
        List<String> cmd = new ArrayList<>(Arrays.asList(
                pythonExe, trainerPy,
                "--client_id", String.valueOf(clientId),
                "--num_clients", String.valueOf(numClients),
                "--seed", String.valueOf(seed),
                "--subset", String.valueOf(subset),
                "--epochs", String.valueOf(epochs),
                "--lr", String.valueOf(lr),
                "--batch_size", String.valueOf(batchSize),
                "--data_dir", dataDir,
                "--out", outBin.toString()
        ));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("[PY] " + line);
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

    private static int parseNumSamplesFromJson(String text) {
        try {
            int l = text.indexOf('{');
            int r = text.lastIndexOf('}');
            if (l >= 0 && r > l) {
                String json = text.substring(l, r + 1);
                JSONObject obj = new JSONObject(json);
                return obj.getInt("num_samples");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

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

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java Client <config.json>");
            System.exit(1);
        }
        Client node = new Client(args[0]);
        java.util.concurrent.CountDownLatch quit = new java.util.concurrent.CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { node.shutdown(); } finally { quit.countDown(); }
        }));
        node.start();
        quit.await();
    }

    public void shutdown() {
        try { if (dp != null) dp.delete_contained_entities(); } catch (Exception ignore) {}
        System.out.println("[JavaDDS] shutdown.");
    }
}
