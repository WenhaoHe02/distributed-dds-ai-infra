package com.example.ocrclient.internal;

import com.example.ocrclient.data_structure.ResultItem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 请求状态类
 * 用于跟踪单个请求的处理状态和结果数据
 */
public class RequestState {
    private final String requestId;
    private final Map<String, ResultItem> receivedResults = new ConcurrentHashMap<>();
    private final long startTime;
    private long expectedTaskCount = -1; // -1表示尚未设置
    private boolean completed = false;

    public RequestState(String requestId) {
        this.requestId = requestId;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * 添加结果项
     *
     * @param item 结果项
     */
    public void addResultItem(ResultItem item) {
        receivedResults.put(item.task_id, item);
        checkCompletion();
    }

    /**
     * 检查是否完成
     */
    private void checkCompletion() {
        if (expectedTaskCount > 0 && receivedResults.size() >= expectedTaskCount) {
            completed = true;
        }
    }

    /**
     * 设置预期任务数
     *
     * @param count 预期任务数
     */
    public void setExpectedTaskCount(long count) {
        this.expectedTaskCount = count;
        checkCompletion();
    }

    /**
     * 获取请求ID
     *
     * @return 请求ID
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * 获取已接收结果数量
     *
     * @return 已接收结果数量
     */
    public int getReceivedResultCount() {
        return receivedResults.size();
    }

    /**
     * 获取预期任务数
     *
     * @return 预期任务数
     */
    public long getExpectedTaskCount() {
        return expectedTaskCount;
    }

    /**
     * 获取开始时间
     *
     * @return 开始时间戳
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * 获取指定任务ID的结果项
     *
     * @param taskId 任务ID
     * @return 结果项，如果不存在则返回null
     */
    public ResultItem getResultItem(String taskId) {
        return receivedResults.get(taskId);
    }

    /**
     * 获取所有已接收的结果项
     *
     * @return 结果项映射
     */
    public Map<String, ResultItem> getReceivedResults() {
        return new ConcurrentHashMap<>(receivedResults);
    }

    /**
     * 更新完成状态并返回
     *
     * @return 是否已完成所有任务
     */
    public boolean updateAndGetCompletion() {
        if (expectedTaskCount <= receivedResults.size()) {
            completed = true;
        }
        return completed;
    }

    /**
     * 是否已完成
     *
     * @return 是否已完成所有任务
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * 计算超时时间（毫秒）
     * 基于任务数计算预期完成时间
     *
     * @return 超时时间（毫秒）
     */
    public long calculateTimeout() {
        if (expectedTaskCount <= 0) {
            // 默认超时时间：30秒
            return 30000;
        }
        // 计算公式：基础时间(5秒) + 每个任务2秒
        return 5000000 + (expectedTaskCount * 2000);
    }

    /**
     * 检查是否超时
     *
     * @return 是否超时
     */
    public boolean isTimeout() {
        long elapsed = System.currentTimeMillis() - startTime;
        return elapsed > calculateTimeout();
    }

    /**
     * 获取处理进度（百分比）
     *
     * @return 处理进度百分比（0-100）
     */
    public int getProgressPercentage() {
        if (expectedTaskCount <= 0) {
            return 0;
        }
        return (int) ((receivedResults.size() * 100) / expectedTaskCount);
    }

    /**
     * 获取进度文本
     *
     * @return 进度文本
     */
    public String getProgressText() {
        if (expectedTaskCount <= 0) {
            return "处理中... (已接收: " + receivedResults.size() + ")";
        }
        return "处理中... (进度: " + receivedResults.size() + "/" + expectedTaskCount +
                " - " + getProgressPercentage() + "%)";
    }
}