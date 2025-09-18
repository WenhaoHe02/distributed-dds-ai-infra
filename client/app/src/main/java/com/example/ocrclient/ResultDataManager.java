package com.example.ocrclient;

import android.util.Log;

import com.example.ocrclient.data_structure.*;
import com.example.ocrclient.data_structure.ResultItem;
import com.example.ocrclient.data_structure.ResultUpdate;
import com.example.ocrclient.internal.RequestState;
import com.zrdds.infrastructure.StringSeq;

import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 结果数据管理器
 * 负责管理分批到达的结果数据，提供超时机制和进度查询功能
 */
public class ResultDataManager {
    private static ResultDataManager instance;
    // 存储已发送的请求的状态
    private final Map<String, RequestState> requestStates = new ConcurrentHashMap<>();
    // 存储已发送的请求ID和客户端ID的映射关系
    private final Map<String, String> sentRequests = new ConcurrentHashMap<>();
    // 存储结果更新监听器
    private final List<ResultUpdateListener> listeners = new CopyOnWriteArrayList<>();

    private static final String TAG = "ResultDataManager";

    private ResultDataManager() {
    }

    // 单例模式管理
    public static synchronized ResultDataManager getInstance() {
        if (instance == null) {
            instance = new ResultDataManager();
        }
        return instance;
    }

    /**
     * 添加结果更新监听器
     *
     * @param listener 监听器
     */
    public void addResultUpdateListener(ResultUpdateListener listener) {
        listeners.add(listener);
        Log.d(TAG, "ResultUpdateListener added, total listeners: " + listeners.size());
    }

    /**
     * 移除结果更新监听器
     *
     * @param listener 监听器
     */
    public void removeResultUpdateListener(ResultUpdateListener listener) {
        listeners.remove(listener);
        Log.d(TAG, "ResultUpdateListener removed, total listeners: " + listeners.size());
    }

    /**
     * 通知所有监听器结果已更新
     *
     * @param requestId 更新的请求ID
     */
    private void notifyResultUpdated(String requestId) {
        Log.d(TAG, "Notifying result update, requestId: " + requestId + ", listener count: " + listeners.size());
        for (ResultUpdateListener listener : listeners) {
            Log.d(TAG, "Calling onResultUpdated on listener: " + listener.getClass().getName());
            listener.onResultUpdated(requestId);
        }
        Log.d(TAG, "Finished notifying result update");
    }

    /**
     * 处理接收到的ResultUpdate数据
     *
     * @param resultUpdate 接收到的结果更新数据
     */
    public void handleResultUpdate(ResultUpdate resultUpdate) {
        String requestId = resultUpdate.request_id;
        String clientId = resultUpdate.client_id;

        Log.d(TAG, "收到ResultUpdate，requestId: " + requestId + ", clientId: " + clientId);
        Log.d(TAG, "当前监听器数量: " + listeners.size());
        Log.d(TAG, "当前requestStates数量: " + requestStates.size());
        Log.d(TAG, "当前sentRequests数量: " + sentRequests.size());

        // 验证消息是否属于自己
        if (!isRequestValid(requestId, clientId)) {
            Log.d(TAG, "请求验证失败，requestId: " + requestId);
            return;
        }

        Log.d(TAG, "请求验证成功，requestId: " + requestId);

        // 获取或创建请求状态
        RequestState requestState = requestStates.computeIfAbsent(requestId,
                id -> {
                    Log.d(TAG, "创建新的RequestState对象，requestId: " + id);
                    return new RequestState(id);
                });

        Log.d(TAG, "获取到RequestState，当前结果数: " + requestState.getReceivedResultCount());

        // 合并新结果
        Log.d(TAG, "开始处理ResultUpdate中的items，数量: " + resultUpdate.items.length());
        for (int i = 0; i < resultUpdate.items.length(); i++) {
            ResultItem item = resultUpdate.items.get_at(i);
            Log.d(TAG, "处理第" + (i + 1) + "个结果项，taskId: " + item.task_id);

            // 深拷贝整个 ResultItem
            ResultItem copyItem = deepCopyResultItem(item);

            Log.d(TAG, "深拷贝完成，添加结果项到requestState");
            requestState.addResultItem(copyItem);
            Log.d(TAG, "结果项添加完成");
        }

        Log.d(TAG, "添加结果后，当前结果数: " + requestState.getReceivedResultCount());

        // 通知监听器结果已更新
        notifyResultUpdated(requestId);
    }

    /**
     * 对 ResultItem 进行完整深拷贝，包括 output_blob 和 texts
     */
    private ResultItem deepCopyResultItem(ResultItem original) {
        Log.d(TAG, "开始深拷贝ResultItem: " + original);
        if (original == null) {
            Log.w(TAG, "原始ResultItem为空，返回null");
            return null;
        }

        ResultItem copy = new ResultItem();
        copy.task_id = original.task_id;
        Log.d(TAG, "拷贝task_id: " + copy.task_id);

        // 深拷贝 output_blob
        if (original.output_blob != null && original.output_blob.value.length() > 0) {
            Log.d(TAG, "output_blob不为空，开始拷贝");
            int len = original.output_blob.value.length();
            Log.d(TAG, "output_blob长度: " + len);
            byte[] byteCopy = new byte[len];
            for (int i = 0; i < len; i++) {
                byteCopy[i] = original.output_blob.value.get_at(i);
            }
            copy.output_blob = new Bytes();
            copy.output_blob.value.from_array(byteCopy, len);
            Log.d(TAG, "output_blob拷贝完成");
        } else {
            copy.output_blob = new Bytes(); // 保证非 null
            Log.d(TAG, "output_blob为空或长度为0，创建新的空Bytes对象");
        }

        // 深拷贝 texts（假设 texts 支持按索引 get_at 和 length）
        if (original.texts != null && original.texts.length() > 0) {
            Log.d(TAG, "texts不为空，开始拷贝");
            Log.d(TAG, "texts长度: " + original.texts.length());
            copy.texts = new StringSeq();
            copy.texts.ensure_length(original.texts.length(), original.texts.length());
            for (int i = 0; i < original.texts.length(); i++) {
                String text = original.texts.get_at(i);
                copy.texts.set_at(i, text);
                Log.d(TAG, "拷贝text[" + i + "]: " + text);
            }
            Log.d(TAG, "texts拷贝完成，总数量: " + copy.texts.length());
        } else {
            copy.texts = new StringSeq();
            Log.d(TAG, "texts为空或长度为0，创建新的空StringSeq对象");
        }

        // 其他字段如果有，也可以在这里拷贝
        // e.g., copy.someOtherField = original.someOtherField;

        Log.d(TAG, "ResultItem深拷贝完成: " + copy);
        return copy;
    }

    /**
     * 获取指定请求的当前状态
     *
     * @param requestId 请求ID
     * @return 请求状态
     */
    public RequestState getRequestState(String requestId) {
        Log.d(TAG, "获取RequestState，requestId: " + requestId + ", 是否存在: " + requestStates.containsKey(requestId));
        if (requestStates.containsKey(requestId)) {
            Log.d(TAG, "RequestState结果数: " + requestStates.get(requestId).getReceivedResultCount());
        }
        return requestStates.get(requestId);
    }

    /**
     * 移除指定请求的状态数据
     *
     * @param requestId 请求ID
     */
    public void removeRequestState(String requestId) {
        requestStates.remove(requestId);
        Log.d(TAG, "移除RequestState，requestId: " + requestId);
    }

    /**
     * 添加已发送的请求，用于验证接收到的消息是否属于自己
     *
     * @param requestId 请求ID
     * @param clientId  客户端ID
     */
    public void addSentRequest(String requestId, String clientId) {
        sentRequests.put(requestId, clientId);
        Log.d(TAG, "添加已发送请求，requestId: " + requestId + ", clientId: " + clientId);
    }

    /**
     * 注册请求状态
     *
     * @param requestState 请求状态
     */
    public void registerRequestState(RequestState requestState) {
        requestStates.put(requestState.getRequestId(), requestState);
        Log.d(TAG, "注册RequestState，requestId: " + requestState.getRequestId() +
                ", 预期任务数: " + requestState.getExpectedTaskCount());
    }

    /**
     * 清理已完成或超时的请求
     *
     * @param requestId 请求ID
     */
    public void cleanupRequest(String requestId) {
        // 清除请求状态列表中匹配的项
        requestStates.remove(requestId);
        // 清除requestId和clientId映射表中匹配的项
        sentRequests.remove(requestId);
        Log.d(TAG, "清理请求，requestId: " + requestId);
    }

    /**
     * 检查请求ID是否在已发送列表中，并且客户端ID是否匹配
     *
     * @param requestId        请求ID
     * @param receivedClientId 接收到的客户端ID
     * @return 是否有效
     */
    private boolean isRequestValid(String requestId, String receivedClientId) {
        Log.d(TAG, "验证请求有效性，requestId: " + requestId +
                ", receivedClientId: " + receivedClientId +
                ", sentRequests size: " + sentRequests.size());

        for (Map.Entry<String, String> entry : sentRequests.entrySet()) {
            Log.d(TAG, "sentRequests entry - key: " + entry.getKey() + ", value: " + entry.getValue());
        }

        boolean isValid = sentRequests.containsKey(requestId) && sentRequests.get(requestId).equals(receivedClientId);
        Log.d(TAG, "验证请求有效性，requestId: " + requestId +
                ", receivedClientId: " + receivedClientId +
                ", 本地clientId: " + sentRequests.get(requestId) +
                ", 是否有效: " + isValid);
        return isValid;
    }

    /**
     * 检查请求是否已完成或超时
     *
     * @param requestId 请求ID
     * @return 是否已完成或超时
     */
    public boolean isRequestFinished(String requestId) {
        RequestState requestState = requestStates.get(requestId);
        if (requestState == null) {
            // 如果请求状态不存在，可能已被清理，视为已完成
            return true;
        }
        return requestState.isCompleted() || requestState.isTimeout();
    }
}