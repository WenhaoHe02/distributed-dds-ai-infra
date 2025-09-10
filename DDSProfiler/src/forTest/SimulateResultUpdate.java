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

public class SimulateResultUpdate {
    private static final String SEND_LOG_PATH = "logs/send.log";
    
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
        
        ReturnCode_t rtn;
        
        // 域号
        int domain_id = 100;
        
        // 创建域参与者
        DomainParticipant dp = DomainParticipantFactory.get_instance().create_participant(
                domain_id, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        if (dp == null) {
            System.out.println("create dp failed");
            return;
        }
        
        // 注册数据类型
        ResultUpdateTypeSupport resultUpdateTypeSupport = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();
        rtn = resultUpdateTypeSupport.register_type(dp, null);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("register type failed");
            cleanupResources(dp, null, null);
            return;
        }
        
        // 创建主题
        Topic tp = dp.create_topic("inference/result_update", resultUpdateTypeSupport.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (tp == null) {
            System.out.println("create tp failed");
            cleanupResources(dp, null, null);
            return;
        }
        
        // 创建发布者
        Publisher pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        if (pub == null) {
            System.out.println("create pub failed");
            cleanupResources(dp, tp, null);
            return;
        }
        
        // 创建数据写者
        DataWriter _dw = pub.create_datawriter(tp, Publisher.DATAWRITER_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        ResultUpdateDataWriter dw = (ResultUpdateDataWriter) (_dw);
        if (dw == null) {
            System.out.println("create dw failed");
            cleanupResources(dp, tp, pub);
            return;
        }
        
        // 读取并处理send.log文件
        try (BufferedReader reader = new BufferedReader(new FileReader(SEND_LOG_PATH))) {
            ObjectMapper mapper = new ObjectMapper();
            String line;
            Random random = new Random();
            
            System.out.println("Start processing send.log and sending ResultUpdate messages...");
            
            // 逐行读取日志文件
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                try {
                    // 解析JSON行
                    JsonNode rootNode = mapper.readTree(line);
                    
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
                        String taskId = taskNode.get("task_id").asText();
                        
                        ResultItem resultItem = new ResultItem();
                        resultItem.task_id = taskId;
                        
                        // 添加一些模拟的推理结果
                        byte[] resultData = generateRandomResultData();
                        // 确保不会超过数组边界
                        resultItem.output_blob.value.from_array(resultData, resultData.length);
                        
                        resultUpdate.items.set_at(i, resultItem);
                    }
                    
                    // 发送ResultUpdate消息
                    rtn = dw.write(resultUpdate, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    if (rtn != ReturnCode_t.RETCODE_OK) {
                        System.out.println("Failed to write ResultUpdate for request: " + requestId);
                    } else {
                        System.out.println("Successfully sent ResultUpdate for request: " + requestId + 
                                         " with " + taskCount + " tasks");
                    }
                    
                    // 添加小延迟以避免发送过快
                    Thread.sleep(100 + random.nextInt(200));
                    
                } catch (Exception e) {
                    System.err.println("Error processing line: " + e.getMessage());
                    System.err.println("Line content: " + line);
                }
            }
            
            System.out.println("Finished processing all entries in send.log");
            
        } catch (Exception e) {
            System.err.println("Error reading or processing send.log: " + e.getMessage());
            e.printStackTrace();
        }
        
        // 释放DDS资源
        cleanupResources(dp, tp, pub);
        System.out.println("Simulation completed successfully.");
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
    private static void cleanupResources(DomainParticipant dp, Topic tp, Publisher pub) {
        try {
            // 删除包含的实体
            if (dp != null) {
                ReturnCode_t rtn = dp.delete_contained_entities();
                if (rtn != ReturnCode_t.RETCODE_OK) {
                    System.out.println("dp delete contained entities failed: " + rtn);
                }
            }
            
            // 删除DomainParticipant
            if (dp != null) {
                ReturnCode_t rtn = DomainParticipantFactory.get_instance().delete_participant(dp);
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