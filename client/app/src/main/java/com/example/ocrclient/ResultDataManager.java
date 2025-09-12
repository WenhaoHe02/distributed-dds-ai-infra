package com.example.ocrclient;

import android.util.Log;

import com.example.ocrclient.data_structure.*;
import com.example.ocrclient.data_structure.ResultItem;
import com.example.ocrclient.data_structure.ResultUpdate;
import com.example.ocrclient.internal.RequestState;

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

    private ResultDataManager() {}

    // 单例模式管理
    public static synchronized ResultDataManager getInstance() {
        if (instance == null) {
            instance = new ResultDataManager();
        }
        return instance;
    }

    /**
     * 添加结果更新监听器
     * @param listener 监听器
     */
    public void addResultUpdateListener(ResultUpdateListener listener) {
        listeners.add(listener);
    }

    /**
     * 移除结果更新监听器
     * @param listener 监听器
     */
    public void removeResultUpdateListener(ResultUpdateListener listener) {
        listeners.remove(listener);
    }

    /**
     * 通知所有监听器结果已更新
     * @param requestId 更新的请求ID
     */
    private void notifyResultUpdated(String requestId) {
        for (ResultUpdateListener listener : listeners) {
            listener.onResultUpdated(requestId);
        }
    }

    /**
     * 处理接收到的ResultUpdate数据
     * @param resultUpdate 接收到的结果更新数据
     */
    public void handleResultUpdate(ResultUpdate resultUpdate) {
        String requestId = resultUpdate.request_id;
        String clientId = resultUpdate.client_id;
        
        Log.d(TAG, "收到ResultUpdate，requestId: " + requestId + ", clientId: " + clientId);
        
        // 验证消息是否属于自己
        if (!isRequestValid(requestId, clientId)) {
            Log.d(TAG, "请求验证失败，requestId: " + requestId);
            return;
        }
        
        Log.d(TAG, "请求验证成功，requestId: " + requestId);
        
        // 获取或创建请求状态
        RequestState requestState = requestStates.computeIfAbsent(requestId, 
            id -> new RequestState(id));
        
        Log.d(TAG, "获取到RequestState，当前结果数: " + requestState.getReceivedResultCount());
        
        // 合并新结果
        for (int i = 0; i < resultUpdate.items.length(); i++) {
            ResultItem item = resultUpdate.items.get_at(i);
            Log.d(TAG, "添加结果项，taskId: " + item.task_id);
            requestState.addResultItem(item);
        }
        
        Log.d(TAG, "添加结果后，当前结果数: " + requestState.getReceivedResultCount());
        
        // 通知监听器结果已更新
        notifyResultUpdated(requestId);
    }
    
    /**
     * 获取指定请求的当前状态
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
     * @param requestId 请求ID
     */
    public void removeRequestState(String requestId) {
        requestStates.remove(requestId);
        Log.d(TAG, "移除RequestState，requestId: " + requestId);
    }
    
    /**
     * 添加已发送的请求，用于验证接收到的消息是否属于自己
     * @param requestId 请求ID
     * @param clientId 客户端ID
     */
    public void addSentRequest(String requestId, String clientId) {
        sentRequests.put(requestId, clientId);
        Log.d(TAG, "添加已发送请求，requestId: " + requestId + ", clientId: " + clientId);
    }

    /**
     * 注册请求状态
     * @param requestState 请求状态
     */
    public void registerRequestState(RequestState requestState) {
        requestStates.put(requestState.getRequestId(), requestState);
        Log.d(TAG, "注册RequestState，requestId: " + requestState.getRequestId() + 
            ", 预期任务数: " + requestState.getExpectedTaskCount());
    }
    
    /**
     * 清理已完成或超时的请求
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
     * @param requestId 请求ID
     * @param receivedClientId 接收到的客户端ID
     * @return 是否有效
     */
    private boolean isRequestValid(String requestId, String receivedClientId) {
        boolean isValid = sentRequests.containsKey(requestId) && sentRequests.get(requestId).equals(receivedClientId);
        Log.d(TAG, "验证请求有效性，requestId: " + requestId + 
            ", receivedClientId: " + receivedClientId + 
            ", 本地clientId: " + sentRequests.get(requestId) + 
            ", 是否有效: " + isValid);
        return isValid;
    }
    
    /**
     * 检查请求是否已完成或超时
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