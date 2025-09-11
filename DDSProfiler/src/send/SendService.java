package send;

import com.zrdds.infrastructure.*;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.topic.Topic;
import com.zrdds.publication.Publisher;
import common.GlobalResourceManager;
import data_structure.*;
import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import data_structure.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import javax.imageio.ImageIO;

public class SendService {
    private static final int DOMAIN_ID = 100;
    private static final String TOPIC_INFER_REQ = "inference/request";

    // 模型配置
    private static final String[] MODELS = { "ocr" };

    // 优先级配置
    private static final int[] PRIORITIES = { 0, 1, 2 };

    // 任务数量配置（现在作为实例变量，可以被构造函数覆盖）
    private int minTasksPerRequest = 1;
    private int maxTasksPerRequest = 2;

    // 图片数据配置
    private static final int MIN_IMAGE_WIDTH = 224;
    private static final int MAX_IMAGE_WIDTH = 500;
    private static final int MIN_IMAGE_HEIGHT = 224;
    private static final int MAX_IMAGE_HEIGHT = 500;

    // 请求间隔配置（现在作为实例变量，可以被构造函数覆盖）
    private int minRequestIntervalMs = 50;
    private int maxRequestIntervalMs = 60;


    
    // 默认请求数量
    private static final int DEFAULT_REQUEST_COUNT = 1;
    
    // 为每个测试运行生成唯一的日志文件名
    private static final String LOG_FILE_NAME = createLogFileName();
    
    // 创建日志文件名并确保目录存在
    private static String createLogFileName() {
        // 确保logs目录存在
        File logsDir = new File("logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }
        return "logs/send_" + System.currentTimeMillis() + ".log";
    }

    private DomainParticipant dp;
    private Publisher pub;
    private Topic topic; // 添加Topic字段
    private InferenceRequestDataWriter requestWriter;
    private PrintWriter logWriter;
    private String clientId;
    private Random random = new Random();
    
    // 获取全局资源管理器实例
    private final GlobalResourceManager resourceManager = GlobalResourceManager.getInstance();


    public SendService(String clientId) throws Exception {
        this.clientId = clientId;

        // 初始化日志文件，使用读写锁保护
        resourceManager.acquireWriteLock();
        try {
            this.logWriter = new PrintWriter(new FileWriter(LOG_FILE_NAME, true));
        } finally {
            resourceManager.releaseWriteLock();
        }

        // 初始化DDS
        initDDS();
    }

    
    // 带配置参数的构造函数
    public SendService(String clientId, 
                      int minTasksPerRequest, int maxTasksPerRequest,
                      int minRequestIntervalMs, int maxRequestIntervalMs) throws Exception {
        this(clientId);
        this.minTasksPerRequest = minTasksPerRequest;
        this.maxTasksPerRequest = maxTasksPerRequest;
        this.minRequestIntervalMs = minRequestIntervalMs;
        this.maxRequestIntervalMs = maxRequestIntervalMs;
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
        dp.enable();

        // 注册类型
        if (InferenceRequestTypeSupport.get_instance().register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            throw new RuntimeException("register_type failed");
        }

        // 创建主题
        topic = dp.create_topic(TOPIC_INFER_REQ,
                InferenceRequestTypeSupport.get_instance().get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (topic == null) {
            throw new RuntimeException("create_topic failed");
        }
        topic.enable();

        // 创建发布者
        pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (pub == null) {
            throw new RuntimeException("create_publisher failed");
        }
        pub.enable();

        // 创建数据写入器
        requestWriter = (InferenceRequestDataWriter) pub.create_datawriter(
                topic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (requestWriter == null) {
            throw new RuntimeException("create_datawriter failed");
        }
        requestWriter.enable();
    }

    public void sendRequest(String requestId, int taskCount, int priority) throws Exception {
        // 创建请求，包含优先级信息
        String requestIdWithPriority = java.util.UUID.randomUUID().toString() + "_priority" + priority;
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
            task.model_id = MODELS[random.nextInt(MODELS.length)];

            // 添加图片数据负载
            task.payload = createImagePayload();

            tasks.set_at(i, task);

            // 记录任务信息用于日志（但暂不记录时间）
            String taskLog = String.format(
                    "{\"task_id\":\"%s\",\"model_id\":\"%s\",\"send_time\":%%d}",
                    task.task_id, task.model_id);
            taskLogs.add(taskLog);
        }

        request.tasks = tasks;

        // 发送请求
        ReturnCode_t rc = requestWriter.write(request, InstanceHandle_t.HANDLE_NIL_NATIVE);

        long sendTimeMs = System.currentTimeMillis(); // 在发送后记录时间
        
        // 记录完整的请求信息到日志 (使用新的JSON格式)，使用读写锁保护
        resourceManager.acquireWriteLock();
        try {
            // 记录完整的请求信息到日志 (使用新的JSON格式)
            StringBuilder logEntry = new StringBuilder();
            logEntry.append("{");
            logEntry.append("\"request_id\":\"").append(requestIdWithPriority).append("\",");
            logEntry.append("\"task_sum\":").append(taskCount).append(",");
            logEntry.append("\"tasks\":[");
            
            for (int i = 0; i < taskLogs.size(); i++) {
                if (i > 0) logEntry.append(",");
                // 在此处插入实际发送时间
                logEntry.append(String.format(taskLogs.get(i), sendTimeMs));
            }
            
            logEntry.append("],");
            logEntry.append("\"client_id\":\"").append(clientId).append("\"");
            logEntry.append("}");

            logWriter.println(logEntry.toString());
            logWriter.flush();
        } finally {
            resourceManager.releaseWriteLock();
        }

        if (rc != ReturnCode_t.RETCODE_OK) {
            System.err.println("[send.SendService] request write failed, rc=" + rc);
        } else {
            System.out.println("[send.SendService] Sent request " + requestIdWithPriority +
                    " with " + taskCount + " tasks" + " with priority " + priority);
        }
    }
    // 发送多种不同类型的任务
    public void sendMixedRequests(int count) throws Exception {
        // 增加全局请求计数
        resourceManager.increaseRequestCount(count);
        
        for (int i = 1; i <= count; i++) {
            String requestId = clientId;

            // 随机任务数量
            int taskCount = random.nextInt(maxTasksPerRequest - minTasksPerRequest + 1) + minTasksPerRequest;

            // 随机选择优先级
            int priority = PRIORITIES[random.nextInt(PRIORITIES.length)];

            sendRequest(requestId, taskCount, priority);

            // 随机间隔
            TimeUnit.MILLISECONDS.sleep(
                    random.nextInt(maxRequestIntervalMs - minRequestIntervalMs + 1) + minRequestIntervalMs);
        }
    }

    // 创建模拟图片数据负载
    private Bytes createImagePayload() {
        Bytes payload = new Bytes();
        
        // 随机生成图片尺寸
        int width = random.nextInt(MAX_IMAGE_WIDTH - MIN_IMAGE_WIDTH + 1) + MIN_IMAGE_WIDTH;
        int height = random.nextInt(MAX_IMAGE_HEIGHT - MIN_IMAGE_HEIGHT + 1) + MIN_IMAGE_HEIGHT;
        
        // 生成有效的PNG图像数据
        byte[] imageData = generatePNGImage(width, height);
        payload.value.from_array(imageData, imageData.length);

        return payload;
    }

    /**
     * 使用BufferedImage生成PNG图像数据
     */
    private byte[] generatePNGImage(int width, int height) {
        try {
            // 创建BufferedImage
            BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            
            // 生成随机颜色的像素数据
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    // 生成随机RGB颜色
                    int red = random.nextInt(256);
                    int green = random.nextInt(256);
                    int blue = random.nextInt(256);
                    int rgb = (red << 16) | (green << 8) | blue;
                    image.setRGB(x, y, rgb);
                }
            }
            
            // 将图像写入字节数组
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "png", baos);
            baos.flush();
            byte[] imageBytes = baos.toByteArray();
            baos.close();
            
            return imageBytes;
        } catch (Exception e) {
            // 如果出现异常，返回一个简单的1x1像素图像
            try {
                BufferedImage image = new BufferedImage(1, 1, BufferedImage.TYPE_INT_RGB);
                image.setRGB(0, 0, 0); // 黑色像素
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(image, "png", baos);
                baos.flush();
                byte[] imageBytes = baos.toByteArray();
                baos.close();
                
                return imageBytes;
            } catch (Exception ex) {
                // 如果还失败，返回空数组
                return new byte[0];
            }
        }
    }

    public void close() {
        try {
            resourceManager.acquireWriteLock();
            try {
                if (logWriter != null) {
                    logWriter.close();
                    logWriter = null;
                }
            } finally {
                resourceManager.releaseWriteLock();
            }

            if (dp != null) {
                dp.delete_contained_entities();
                dp = null;
            }

        } catch (Exception e) {
            System.err.println("Error during resource cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SendService sender = null;
        try {
            String clientId = System.getProperty("client.id", "client-" +
                    Integer.toUnsignedString(new Random().nextInt(), 36));

            sender = new SendService(clientId);
            sender.resourceManager.setFilePath(LOG_FILE_NAME);


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