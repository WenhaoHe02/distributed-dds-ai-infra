package com.example.ocrclient;

/**
 * 结果更新监听器接口
 * 用于通知UI组件数据已更新
 */
public interface ResultUpdateListener {
    /**
     * 当结果数据更新时调用
     * @param requestId 更新的请求ID
     */
    void onResultUpdated(String requestId);
}