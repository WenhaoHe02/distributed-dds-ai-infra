package receive;

import com.fasterxml.jackson.databind.JsonNode;
import inner.Request;
import inner.SendLog;
import utils.JsonUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadSendLog {
    private static final String SEND_LOG_PATH = "./logs/send.log";

    private long filePointer = 0;  // 上次读取的位置

    // 存放所有SendLog
    private final Map<String, SendLog> sendLogs = new HashMap<>();

    // 存放所有请求的数据, key为 request_id:client_id
    private final Map<String, Request> requests = new HashMap<>();

    /**
     * 增量读取新日志，每行一个完整的 JSON
     */
    public void updateRead() {
        try (RandomAccessFile raf = new RandomAccessFile(SEND_LOG_PATH, "r")) {
            raf.seek(filePointer);  // 从上次结束位置开始读

            String line;
            while ((line = raf.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue; // 跳过空行

                try {
                    // 直接将每行 JSON 转成 JsonNode
                    JsonNode node = JsonUtil.json2Node(line);

                    // JsonNode 转成已有类
                    SendLog sendLog = JsonUtil.node2Object(node, SendLog.class);
                    sendLogs.put(sendLog.request_id + ":" + sendLog.client_id, sendLog);

                    // 存入 Map
                    Request request = new Request(sendLog);
                    requests.put(sendLog.request_id + ":" + sendLog.client_id, request);
                    //System.out.println("成功读取JSON对象");
                } catch (Exception ex) {
                    System.err.println("解析JSON失败: " + ex.getMessage());
                }
            }

            // 更新文件指针，下次继续从这里读
            filePointer = raf.getFilePointer();
        } catch (FileNotFoundException e) {
            System.err.println("日志文件未找到: " + SEND_LOG_PATH);
        } catch (IOException e) {
            System.err.println("读取日志文件失败: " + e.getMessage());
        }
    }

    /**
     * 根据 request_id 获取 Request
     */
    public Request getSendLog(String requestId, String clientId) {
        return requests.get(requestId + ":" + clientId);
    }

    /**
     * 清空已存储的 SendLog
     */
    public void clearLogs() {
        sendLogs.clear();
    }

    /**
     * 根据 requestId 和 clientId 删除 SendLog
     */
    public void deleteSendLog(String requestId, String clientId) {
        sendLogs.remove(requestId + ":" + clientId);
    }

    /**
     * 判断整个测试是否结束
     * （暂时通过看SendLogs是否为空来判断该次测试是否结束）
     */
    public boolean isTestEnd() {
        return sendLogs.isEmpty();
    }

    public List<Request> getRequests() {
        return new ArrayList<>(requests.values());
    }
}