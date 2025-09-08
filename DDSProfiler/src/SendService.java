import com.zrdds.infrastructure.*;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.topic.Topic;
import com.zrdds.publication.Publisher;
import data_structure.*;
import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import data_structure.*;
import data_structure.Bytes;
import java.util.ArrayList;
import java.util.List;


public class SendService {
    private static final int DOMAIN_ID = 100;
    private static final String TOPIC_INFER_REQ = "inference/request";

    private DomainParticipant dp;
    private Publisher pub;
    private InferenceRequestDataWriter requestWriter;
    private PrintWriter logWriter;
    private String clientId;
    private Random random = new Random();

    // 模型配置
    private static final String[] MODELS = { "modelA", "modelB" };

    // 优先级配置
    private static final int[] PRIORITIES = { 0, 1, 2 };

    // 任务数量配置
    private static final int MIN_TASKS_PER_REQUEST = 1;
    private static final int MAX_TASKS_PER_REQUEST = 10;

    // 图片数据配置
    private static final int MIN_IMAGE_WIDTH = 224;
    private static final int MAX_IMAGE_WIDTH = 1920;
    private static final int MIN_IMAGE_HEIGHT = 224;
    private static final int MAX_IMAGE_HEIGHT = 1080;
    private static final int IMAGE_CHANNELS = 3; // RGB图像

    // 请求间隔配置（毫秒）
    private static final int MIN_REQUEST_INTERVAL_MS = 50;
    private static final int MAX_REQUEST_INTERVAL_MS = 500;

    // 默认请求数量
    private static final int DEFAULT_REQUEST_COUNT = 20;

    public SendService(String clientId) throws Exception {
        this.clientId = clientId;

        // 初始化日志文件
        this.logWriter = new PrintWriter(new FileWriter("logs/send.log", true));

        // 初始化DDS
        initDDS();
    }

    private void initDDS() throws Exception {
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        if (dpf == null) {
            throw new RuntimeException("DomainParticipantFactory.get_instance() failed");
        }

        dp = dpf.create_participant(DOMAIN_ID,
                DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (dp == null) {
            throw new RuntimeException("create_participant failed");
        }

        // 注册类型
        if (InferenceRequestTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            throw new RuntimeException("register_type failed");
        }

        // 创建主题
        Topic reqTopic = dp.create_topic(TOPIC_INFER_REQ,
                InferenceRequestTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (reqTopic == null) {
            throw new RuntimeException("create_topic failed");
        }

        // 创建发布者
        pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (pub == null) {
            throw new RuntimeException("create_publisher failed");
        }

        // 创建数据写入器
        requestWriter = (InferenceRequestDataWriter) pub.create_datawriter(
                reqTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (requestWriter == null) {
            throw new RuntimeException("create_datawriter failed");
        }
    }

    public void sendRequest(String requestId, int taskCount, String modelId, int priority) throws Exception {
        // 创建请求，包含优先级信息
        String requestIdWithPriority = requestId + "-priority" + priority;
        InferenceRequest request = new InferenceRequest();
        request.request_id = requestIdWithPriority;

        // 创建任务序列
        SingleTaskSeq tasks = new SingleTaskSeq();
        tasks.ensure_length(taskCount, taskCount);

        // 存储任务信息用于日志记录
        List<String> taskLogs = new ArrayList<>();

        // 为每个任务记录日志
        for (int i = 0; i < taskCount; i++) {
            SingleTask task = new SingleTask();
            task.request_id = requestIdWithPriority;
            task.task_id = String.format("t%03d", (i + 1));
            task.client_id = clientId;
            task.model_id = modelId;

            // 添加图片数据负载
            task.payload = createImagePayload();

            tasks.set_at(i, task);

            // 记录任务信息用于日志
            long sendTimeMs = System.currentTimeMillis();
            String taskLog = String.format(
                    "{\"task_id\":\"%s\",\"model_id\":\"%s\",\"send_time\":%d}",
                    task.task_id, task.model_id, sendTimeMs);
            taskLogs.add(taskLog);
        }

        // 记录完整的请求信息到日志 (使用新的JSON格式)
        StringBuilder logEntry = new StringBuilder();
        logEntry.append("{");
        logEntry.append("\"request_id\":\"").append(requestIdWithPriority).append("\",");
        logEntry.append("\"task_sum\":").append(taskCount).append(",");
        logEntry.append("\"tasks\":[");
        
        for (int i = 0; i < taskLogs.size(); i++) {
            if (i > 0) logEntry.append(",");
            logEntry.append(taskLogs.get(i));
        }
        
        logEntry.append("],");
        logEntry.append("\"client_id\":\"").append(clientId).append("\"");
        logEntry.append("}");

        logWriter.println(logEntry.toString());
        logWriter.flush();

        request.tasks = tasks;

        // 发送请求
        ReturnCode_t rc = requestWriter.write(request, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[SendService] request write failed, rc=" + rc);
        } else {
            System.out.println("[SendService] Sent request " + requestIdWithPriority +
                    " with " + taskCount + " tasks on model " + modelId + " with priority " + priority);
        }
    }

    // 重载方法，随机选择模型和优先级
    public void sendRequest(String requestId, int taskCount) throws Exception {
        String modelId = MODELS[random.nextInt(MODELS.length)];
        int priority = PRIORITIES[random.nextInt(PRIORITIES.length)];
        sendRequest(requestId, taskCount, modelId, priority);
    }

    // 发送多种不同类型的任务
    public void sendMixedRequests(int count) throws Exception {
        for (int i = 1; i <= count; i++) {
            String requestId = "r" + String.format("%03d", i);

            // 随机任务数量
            int taskCount = random.nextInt(MAX_TASKS_PER_REQUEST - MIN_TASKS_PER_REQUEST + 1) + MIN_TASKS_PER_REQUEST;

            // 随机选择模型
            String modelId = MODELS[random.nextInt(MODELS.length)];

            // 随机选择优先级
            int priority = PRIORITIES[random.nextInt(PRIORITIES.length)];

            sendRequest(requestId, taskCount, modelId, priority);

            // 随机间隔
            TimeUnit.MILLISECONDS.sleep(
                    random.nextInt(MAX_REQUEST_INTERVAL_MS - MIN_REQUEST_INTERVAL_MS + 1) + MIN_REQUEST_INTERVAL_MS);
        }
    }

    // 创建模拟图片数据负载
    private Bytes createImagePayload() {
        Bytes payload = new Bytes();
        
        // 随机生成图片尺寸
        int width = random.nextInt(MAX_IMAGE_WIDTH - MIN_IMAGE_WIDTH + 1) + MIN_IMAGE_WIDTH;
        int height = random.nextInt(MAX_IMAGE_HEIGHT - MIN_IMAGE_HEIGHT + 1) + MIN_IMAGE_HEIGHT;
        
        // 计算图片数据大小 (width * height * channels)
        int size = width * height * IMAGE_CHANNELS;
        
        // 生成模拟的图片数据
        byte[] imageData = new byte[size];
        // 模拟图片数据，这里使用随机数据代替真实图片
        random.nextBytes(imageData);
        payload.value.from_array(imageData, imageData.length);

        return payload;
    }

    public void close() {
        try {
            if (logWriter != null) {
                logWriter.close();
            }

            // 清理DDS资源
            if (requestWriter != null) {
                pub.delete_datawriter(requestWriter);
            }
            if (pub != null) {
                dp.delete_publisher(pub);
            }
            if (dp != null) {
                DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
                dpf.delete_participant(dp);
            }
            DDSIF.Finalize();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SendService sender = null;
        try {
            String clientId = System.getProperty("client.id", "client-" +
                    Integer.toUnsignedString(new Random().nextInt(), 36));

            sender = new SendService(clientId);

            // 发送混合测试请求
            int requestCount = Integer
                    .parseInt(System.getProperty("request.count", String.valueOf(DEFAULT_REQUEST_COUNT)));
            sender.sendMixedRequests(requestCount);

            System.out.println("Requests sent. Press ENTER to exit...");
            System.in.read();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sender != null) {
                sender.close();
            }
        }
    }
}