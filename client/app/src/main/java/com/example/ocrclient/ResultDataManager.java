package com.example.ocrclient;

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
    private final Map<String, RequestState> requestStates = new ConcurrentHashMap<>();
    private final List<ResultDataListener> listeners = new CopyOnWriteArrayList<>();
    // 存储已发送的请求ID和客户端ID的映射关系
    private final Map<String, String> sentRequests = new ConcurrentHashMap<>();

    private ResultDataManager() {}

    public static synchronized ResultDataManager getInstance() {
        if (instance == null) {
            instance = new ResultDataManager();
        }
        return instance;
    }

    /**
     * 处理接收到的ResultUpdate数据
     * @param resultUpdate 接收到的结果更新数据
     */
    public void handleResultUpdate(ResultUpdate resultUpdate) {
        String requestId = resultUpdate.request_id;
        String clientId = resultUpdate.client_id;
        
        // 验证消息是否属于自己
        if (!isRequestValid(requestId, clientId)) {
            return;
        }
        
        // 获取或创建请求状态
        RequestState requestState = requestStates.computeIfAbsent(requestId, 
            id -> new RequestState(id));
        
        // 合并新结果
        for (int i = 0; i < resultUpdate.items.length(); i++) {
            ResultItem item = resultUpdate.items.get_at(i);
            requestState.addResultItem(item);
        }
        
        // 通知监听器
        notifyProgressUpdate(requestId, requestState);
        
        // 检查是否完成
        if (requestState.isCompleted()) {
            notifyRequestCompleted(requestId, requestState);
        }
    }
    
    /**
     * 注册结果数据监听器
     * @param listener 监听器
     */
    public void addListener(ResultDataListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }
    
    /**
     * 移除结果数据监听器
     * @param listener 监听器
     */
    public void removeListener(ResultDataListener listener) {
        listeners.remove(listener);
    }
    
    /**
     * 获取指定请求的当前状态
     * @param requestId 请求ID
     * @return 请求状态
     */
    public RequestState getRequestState(String requestId) {
        return requestStates.get(requestId);
    }
    
    /**
     * 移除指定请求的状态数据
     * @param requestId 请求ID
     */
    public void removeRequestState(String requestId) {
        requestStates.remove(requestId);
    }
    
    /**
     * 添加已发送的请求，用于验证接收到的消息是否属于自己
     * @param requestId 请求ID
     * @param clientId 客户端ID
     */
    public void addSentRequest(String requestId, String clientId) {
        sentRequests.put(requestId, clientId);
    }
    
    /**
     * 注册请求状态
     * @param requestState 请求状态
     */
    public void registerRequestState(RequestState requestState) {
        requestStates.put(requestState.getRequestId(), requestState);
    }
    
    /**
     * 检查请求ID是否在已发送列表中，并且客户端ID是否匹配
     * @param requestId 请求ID
     * @param receivedClientId 接收到的客户端ID
     * @return 是否有效
     */
    private boolean isRequestValid(String requestId, String receivedClientId) {
        return sentRequests.containsKey(requestId) && sentRequests.get(requestId).equals(receivedClientId);
    }
    
    /**
     * 通知进度更新
     * @param requestId 请求ID
     * @param state 请求状态
     */
    private void notifyProgressUpdate(String requestId, RequestState state) {
        for (ResultDataListener listener : listeners) {
            listener.onProgressUpdate(requestId, state);
        }
    }
    
    /**
     * 通知请求完成
     * @param requestId 请求ID
     * @param state 请求状态
     */
    private void notifyRequestCompleted(String requestId, RequestState state) {
        for (ResultDataListener listener : listeners) {
            listener.onRequestCompleted(requestId, state);
        }
    }
    
    /**
     * 通知请求超时
     * @param requestId 请求ID
     * @param state 请求状态
     */
    public void notifyRequestTimeout(String requestId, RequestState state) {
        for (ResultDataListener listener : listeners) {
            listener.onRequestTimeout(requestId, state);
        }
    }
    
    /**
     * 请求状态接口
     */
    public interface ResultDataListener {
        /**
         * 进度更新回调
         * @param requestId 请求ID
         * @param state 请求状态
         */
        void onProgressUpdate(String requestId, RequestState state);
        
        /**
         * 请求完成回调
         * @param requestId 请求ID
         * @param state 请求状态
         */
        void onRequestCompleted(String requestId, RequestState state);
        
        /**
         * 请求超时回调
         * @param requestId 请求ID
         * @param state 请求状态
         */
        void onRequestTimeout(String requestId, RequestState state);
    }
}