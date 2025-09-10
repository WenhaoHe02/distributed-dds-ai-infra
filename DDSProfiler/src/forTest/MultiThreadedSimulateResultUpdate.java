package forTest;

import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.StatusKind;
import com.zrdds.publication.DataWriter;
import com.zrdds.publication.Publisher;
import com.zrdds.topic.Topic;
import data_structure.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiThreadedSimulateResultUpdate {
    private static final String SEND_LOG_PATH = "logs/send.log";
    private static final int THREAD_COUNT = 5; // 线程数
    
    public static void main(String[] args) {
        // 加载DDS库
        loadLibrary();
        
        // 检查send.log文件是否存在
        File sendLogFile = new File(SEND_LOG_PATH);
        if (!sendLogFile.exists()) {
            System.err.println("send.log file not found at: " + SEND_LOG_PATH);
            System.err.println("Please run the SendService or MultiThreadedClientSimulator first to generate the log file.");
            return;
        }
        
        // 读取并解析send.log文件
        List<JsonNode> logEntries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(SEND_LOG_PATH))) {
            ObjectMapper mapper = new ObjectMapper();
            String line;
            
            System.out.println("Reading send.log file...");
            
            // 逐行读取日志文件
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                try {
                    // 解析JSON行
                    JsonNode rootNode = mapper.readTree(line);
                    logEntries.add(rootNode);
                } catch (Exception e) {
                    System.err.println("Error parsing line: " + e.getMessage());
                    System.err.println("Line content: " + line);
                }
            }
            
            System.out.println("Read " + logEntries.size() + " entries from send.log");
            
        } catch (Exception e) {
            System.err.println("Error reading send.log: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        
        if (logEntries.isEmpty()) {
            System.err.println("No valid entries found in send.log");
            return;
        }
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        
        // 创建共享的DDS资源
        SharedDDSResources sharedResources = initDDSResources();
        if (sharedResources == null) {
            System.err.println("Failed to initialize DDS resources");
            return;
        }
        
        System.out.println("Starting multi-threaded ResultUpdate simulation with " + THREAD_COUNT + " threads...");
        
        Random random = new Random();
        
        // 提交任务到线程池
        for (int i = 0; i < logEntries.size(); i++) {
            JsonNode entry = logEntries.get(i);
            int taskId = i;
            
            executor.submit(() -> {
                try {
                    processLogEntry(sharedResources.writer, entry, taskId);
                    
                    // 添加随机延迟
                    Thread.sleep(50 + random.nextInt(150));
                } catch (Exception e) {
                    System.err.println("Error processing task " + taskId + ": " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        
        // 关闭线程池
        executor.shutdown();
        
        try {
            // 等待所有任务完成
            if (executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.out.println("All tasks completed successfully");
            } else {
                System.err.println("Timeout waiting for tasks to complete");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for tasks to complete: " + e.getMessage());
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 清理DDS资源
        cleanupDDSResources(sharedResources);
        System.out.println("Multi-threaded simulation completed successfully.");
    }
    
    /**
     * 初始化共享的DDS资源
     */
    private static SharedDDSResources initDDSResources() {
        try {
            ReturnCode_t rtn;
            
            // 域号
            int domain_id = 100;
            
            // 创建域参与者
            DomainParticipant dp = DomainParticipantFactory.get_instance().create_participant(
                    domain_id, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null,
                    StatusKind.STATUS_MASK_NONE);
            if (dp == null) {
                System.out.println("create dp failed");
                return null;
            }
            
            // 注册数据类型
            ResultUpdateTypeSupport resultUpdateTypeSupport = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();
            rtn = resultUpdateTypeSupport.register_type(dp, null);
            if (rtn != ReturnCode_t.RETCODE_OK) {
                System.out.println("register type failed");
                cleanupOnError(dp, null, null);
                return null;
            }
            
            // 创建主题
            Topic tp = dp.create_topic("inference/result_update", resultUpdateTypeSupport.get_type_name(),
                    DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
            if (tp == null) {
                System.out.println("create tp failed");
                cleanupOnError(dp, null, null);
                return null;
            }
            
            // 创建发布者
            Publisher pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null,
                    StatusKind.STATUS_MASK_NONE);
            if (pub == null) {
                System.out.println("create pub failed");
                cleanupOnError(dp, tp, null);
                return null;
            }
            
            // 创建数据写者
            DataWriter _dw = pub.create_datawriter(tp, Publisher.DATAWRITER_QOS_DEFAULT, null,
                    StatusKind.STATUS_MASK_NONE);
            ResultUpdateDataWriter dw = (ResultUpdateDataWriter) (_dw);
            if (dw == null) {
                System.out.println("create dw failed");
                cleanupOnError(dp, tp, pub);
                return null;
            }
            
            return new SharedDDSResources(dp, tp, pub, dw);
        } catch (Exception e) {
            System.err.println("Error initializing DDS resources: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * 处理单个日志条目
     */
    private static void processLogEntry(ResultUpdateDataWriter writer, JsonNode rootNode, int taskId) {
        try {
            // 提取请求信息
            String requestId = rootNode.get("request_id").asText();
            String clientId = rootNode.get("client_id").asText();
            JsonNode tasksNode = rootNode.get("tasks");
            
            // 创建ResultUpdate数据
            ResultUpdate resultUpdate = new ResultUpdate();
            resultUpdate.request_id = requestId;
            resultUpdate.client_id = clientId;
            
            // 处理任务项
            int taskCount = tasksNode.size();
            resultUpdate.items.ensure_length(taskCount, taskCount);
            
            for (int i = 0; i < taskCount; i++) {
                JsonNode taskNode = tasksNode.get(i);
                String taskIdStr = taskNode.get("task_id").asText();
                
                ResultItem resultItem = new ResultItem();
                resultItem.task_id = taskIdStr;
                
                // 添加一些模拟的推理结果
                byte[] resultData = generateRandomResultData();
                // 确保不会超过数组边界
                resultItem.output_blob.value.from_array(resultData, resultData.length);
                
                resultUpdate.items.set_at(i, resultItem);
            }
            
            // 发送ResultUpdate消息
            ReturnCode_t rtn = writer.write(resultUpdate, InstanceHandle_t.HANDLE_NIL_NATIVE);
            if (rtn != ReturnCode_t.RETCODE_OK) {
                System.out.println("Failed to write ResultUpdate for request: " + requestId + " (task " + taskId + ")");
            } else {
                System.out.println("Successfully sent ResultUpdate for request: " + requestId + 
                                 " with " + taskCount + " tasks (task " + taskId + ")");
            }
        } catch (Exception e) {
            System.err.println("Error processing log entry " + taskId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 生成随机的推理结果数据
     */
    private static byte[] generateRandomResultData() {
        Random random = new Random();
        // 使用较小的长度以避免数组越界问题
        int length = 50 + random.nextInt(100); // 50-150字节的随机数据
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }
    
    /**
     * 清理DDS资源
     */
    private static void cleanupDDSResources(SharedDDSResources resources) {
        try {
            // 删除包含的实体
            if (resources.dp != null) {
                ReturnCode_t rtn = resources.dp.delete_contained_entities();
                if (rtn != ReturnCode_t.RETCODE_OK) {
                    System.out.println("dp delete contained entities failed: " + rtn);
                }
            }
            
            // 删除DomainParticipant
            if (resources.dp != null) {
                ReturnCode_t rtn = DomainParticipantFactory.get_instance().delete_participant(resources.dp);
                if (rtn != ReturnCode_t.RETCODE_OK) {
                    System.out.println("dpf delete dp failed: " + rtn);
                }
            }
            
            // 终止DomainParticipantFactory
            DomainParticipantFactory.finalize_instance();
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 出错时的资源清理
     */
    private static void cleanupOnError(DomainParticipant dp, Topic tp, Publisher pub) {
        try {
            // 删除包含的实体
            if (dp != null) {
                dp.delete_contained_entities();
            }
            
            // 删除DomainParticipant
            if (dp != null) {
                DomainParticipantFactory.get_instance().delete_participant(dp);
            }
            
            // 终止DomainParticipantFactory
            DomainParticipantFactory.finalize_instance();
        } catch (Exception e) {
            System.err.println("Error during error cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 共享DDS资源类
     */
    private static class SharedDDSResources {
        final DomainParticipant dp;
        final Topic topic;
        final Publisher pub;
        final ResultUpdateDataWriter writer;
        
        SharedDDSResources(DomainParticipant dp, Topic topic, Publisher pub, ResultUpdateDataWriter writer) {
            this.dp = dp;
            this.topic = topic;
            this.pub = pub;
            this.writer = writer;
        }
    }
    
    // 已加载库标识
    private static boolean hasLoad = false;
    
    // 加载DDS库
    private static void loadLibrary() {
        if (!hasLoad) {
            try {
                System.loadLibrary("ZRDDS_JAVA");
                hasLoad = true;
                System.out.println("ZRDDS library loaded successfully.");
            } catch (UnsatisfiedLinkError e) {
                System.err.println("Failed to load ZRDDS library: " + e.getMessage());
            }
        }
    }
}