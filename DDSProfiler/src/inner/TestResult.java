package inner;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * 存储性能测试结果的数据结构
 * 包含吞吐量、端到端时间、P90延迟以及每种任务的平均延迟等指标
 */
@Data
public class TestResult {
    // TODO: 是否应该重新排序，便于展示？
    // 吞吐量 (单位时间完成的任务数)(每秒)
    public double requestThroughput;
    public double taskThroughput;

    // 端到端时间（目前保留request结果，不做进一步计算）
    public List<Request> requests = new ArrayList<>();
    public long totalTime; // 整个测试总耗时

    // P90延迟 (以task为单位)
    public long p90Latency;

    // 每种任务（根据model_id划分）的平均延迟
    public Map<String, Double> averageLatencyByModel;

    // 所有任务的延迟列表，用于计算P90等统计指标（已按耗时降序排序）
    public List<Long> allTaskLatencies;

    /**
     * 默认构造函数
     */
    public TestResult() {
        this.averageLatencyByModel = new HashMap<>();
        this.allTaskLatencies = new ArrayList<>();
    }

    /**
     * 拷贝构造函数
     *
     * @param other 其他TestResult对象
     */
    public TestResult(TestResult other) {
        this.requestThroughput = other.requestThroughput;
        this.taskThroughput = other.taskThroughput;
        this.p90Latency = other.p90Latency;

        this.averageLatencyByModel = new HashMap<>(other.averageLatencyByModel);
        this.allTaskLatencies = new ArrayList<>(other.allTaskLatencies);
    }

    /**
     * 接收 Map<Object, Request>，赋值给 this.requests
     */
    public void setRequests(Map<Object, Request> requests) {
        this.requests = new ArrayList<>(requests.values());
    }
}