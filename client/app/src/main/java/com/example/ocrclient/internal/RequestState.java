package com.example.ocrclient.internal;

import android.util.Log;

import com.example.ocrclient.*;
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

    // 第一个任务超时时间（毫秒）- 400ms
    private static final long FIRST_TASK_TIMEOUT = 4000;

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
        Log.d("addResultItem", "添加结果项: " + item.task_id + ", item=" + item);
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
        return 5000 + (expectedTaskCount * 2000);
    }

    /**
     * 检查是否超时（整体超时）
     *
     * @return 是否超时
     */
    public boolean isTimeout() {
        long elapsed = System.currentTimeMillis() - startTime;
        return elapsed > calculateTimeout();
    }

    /**
     * 检查第一个任务是否超时
     * 如果在规定时间内（10秒）没有收到任何结果，则判定为超时
     *
     * @return 第一个任务是否超时
     */
    public boolean isFirstTaskTimeout() {
        // 如果已经收到了结果，则不会第一个任务超时
        if (receivedResults.size() > 0) {
            return false;
        }
        
        // 检查是否超过了第一个任务的超时时间
        long elapsed = System.currentTimeMillis() - startTime;
        return elapsed > FIRST_TASK_TIMEOUT;
    }

    /**
     * 检查是否超时（包括整体超时和第一个任务超时）
     *
     * @return 是否超时
     */
    public boolean isAnyTimeout() {
        return isFirstTaskTimeout() || isTimeout();
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
        // 如果没有超时
        if(!isAnyTimeout()) {
            if(expectedTaskCount>receivedResults.size()) {
                // 没有全部处理完成
                return "处理中... (进度: " + receivedResults.size() + "/" + expectedTaskCount +
                        " - " + getProgressPercentage() + "%)";
            }
            else{
                return "处理完成 (进度: " + receivedResults.size() + "/" + expectedTaskCount +
                    " - " + getProgressPercentage() + "%)";
            }
        }
        else{
            return "处理超时 (进度: " + receivedResults.size() + "/" + expectedTaskCount +
                    " - " + getProgressPercentage() + "%)";
        }
    }
}