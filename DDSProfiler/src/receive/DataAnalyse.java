package receive;

import common.GlobalResourceManager;
import data_structure.ResultItem;
import data_structure.ResultUpdate;
import inner.TestResult;
import inner.Request;
import utils.JsonUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class DataAnalyse {
    private final ReadSendLog readSendLog;
    private long testStartTime = Long.MAX_VALUE; // 整个测试开始时间
    private long testEndTime; // 整个测试结束时间

    private int solvedRequests = 0;

    public DataAnalyse() {
        readSendLog = new ReadSendLog();
    }

    // 处理ReusltUpdate
    public void processResultUpdate(ResultUpdate resultUpdate, long receiveTime) {
        System.out.println("[PROCESS_RESULT_UPDATE] 开始处理ResultUpdate: request_id=" + resultUpdate.request_id +
                ", client_id=" + resultUpdate.client_id + ", receiveTime=" + receiveTime);

        // 更新发送日志信息
        readSendLog.updateRead();
        System.out.println("[READ_SEND_LOG] 已更新发送日志信息");

        // TODO: 考虑处理失败的数据的统计，还有超时判断？
        // 解析，更新列表数据（同步更新时间字段）（这个设计是否合适）
        Request request = readSendLog.getRequest(resultUpdate.request_id, resultUpdate.client_id);
        if (request == null) {
            System.out.println("[REQUEST_NOT_FOUND] request_id: " + resultUpdate.request_id + " client_id: " + resultUpdate.client_id + " 没有找到");
            //TODO: 异常处理

            return;
        }
        System.out.println("[REQUEST_FOUND] 接收到已知request: request_id=" + resultUpdate.request_id +
                ", client_id=" + resultUpdate.client_id + ", task_sum=" + request.task_sum);

        // 解析items
        System.out.println("[PROCESS_ITEMS] 开始处理items，items长度: " + resultUpdate.items.length());
        for (int i = 0; i < resultUpdate.items.length(); i++) {
            ResultItem item = resultUpdate.items.get_at(i);
            
            // 增加调试信息，检查item是否为null以及task_id是否为空
            if (item == null) {
                System.out.println("[ITEM_NULL] 第" + (i+1) + "个item为null");
                continue;
            }
            
            System.out.println("[ITEM_DEBUG] 处理第" + (i+1) + "个item: task_id=" + item.task_id + ", status=" + item.status);
            
            // 检查task_id是否为空
            if (item.task_id == null || item.task_id.isEmpty()) {
                System.out.println("[TASK_ID_EMPTY] 第" + (i+1) + "个item的task_id为空");
                continue;
            }
            
            Request.Task task = request.tasks.get(item.task_id);

            if (task == null) {
                //TODO: 异常处理
                System.out.println("[TASK_NOT_FOUND] 接收到未知task: task_id=" + item.task_id);
                continue; // 改为continue而不是return，继续处理其他task
            }

            System.out.println("[TASK_FOUND] 接收到已知task: task_id=" + item.task_id +
                    ", model_id=" + task.model_id + ", send_time=" + task.send_time);
            task.finish_time = receiveTime;
            task.cost_time = task.finish_time - task.send_time;
            request.cur_total++;

            // 更新测试开始时间
            testStartTime = Math.min(task.send_time, testStartTime);
            System.out.println("[TASK_PROCESSED] task处理完成: task_id=" + item.task_id +
                    ", finish_time=" + task.finish_time + ", cost_time=" + task.cost_time +
                    ", cur_total=" + request.cur_total);
        }

        // 判断request是否完成
        System.out.println("[CHECK_REQUEST_COMPLETION] 检查request是否完成: cur_total=" + request.cur_total +
                ", task_sum=" + request.task_sum);

        // request未完成直接返回，继续等待，不再判断测试是否完成
        if (request.cur_total < request.task_sum) {
            System.out.println("[REQUEST_NOT_COMPLETED] request未完成: cur_total=" + request.cur_total +
                    ", task_sum=" + request.task_sum);
            return;
        }

        // request完成，更新测试完成时间
        System.out.println("[REQUEST_COMPLETED] request完成: request_id=" + resultUpdate.request_id);
        request.is_done = true;
        // 如果该request完成，计算该request耗时（取所有task中的最长耗时）
        long min_start = Long.MAX_VALUE;
        long max_finish = Long.MIN_VALUE;
        for (Request.Task task : request.tasks.values()) {
            if (task.send_time < min_start) {
                min_start = task.send_time;
            }
            if (task.finish_time > max_finish) {
                max_finish = task.finish_time;
            }
        }
        request.total_time = max_finish - min_start;
        System.out.println("[REQUEST_TIME_CALCULATED] request耗时计算完成: total_time=" + request.total_time +
                ", min_start=" + min_start + ", max_finish=" + max_finish);

        // 更新已完成的请求数
        solvedRequests++;

        // 判断整个测试是否完成
        boolean isTestEnd = isTestEnd();
        System.out.println("[CHECK_TEST_COMPLETION] 检查整个测试是否完成: isTestEnd=" + isTestEnd);
        if (isTestEnd) {
            System.out.println("[TEST_COMPLETED] 测试完成！");

            // 更新测试结束时间
            testEndTime = System.currentTimeMillis();
            System.out.println("[TEST_END_TIME] 测试结束时间: " + testEndTime);

            // 生成测试结果
            TestResult testResult = generateTestResult();
            System.out.println("[TEST_RESULT] 测试结果：" + testResult);
            // 把结果转化成Json保存到logs中（带当前时间戳命名）
            String json = JsonUtil.object2Json(testResult);
            String fileName = "test_result_" + System.currentTimeMillis() + ".json";
            Path filePath = Paths.get("./logs/results/" + fileName);
            try {
                Files.createDirectories(filePath.getParent());
                Files.write(filePath, json.getBytes());
                System.out.println("[RESULT_SAVED] 测试结果已保存到文件: " + filePath.toString());
            } catch (IOException e) {
                System.err.println("[SAVE_RESULT_ERROR] 保存测试结果失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("[PROCESS_RESULT_UPDATE] ResultUpdate处理完成: request_id=" + resultUpdate.request_id);
        GlobalResourceManager.getInstance().setTestEnd();
    }

    /**
     * 生成测试结果
     */
    private TestResult generateTestResult() {
        TestResult result = new TestResult();

        // 计算各种指标
        calculateThroughput(result);
        calculateEndToEndTime(result);
        calculateP90Latency(result);
        calculateAverageLatencyByModel(result);

        return result;
    }

    /**
     * 计算吞吐量
     *
     * @param result 测试结果对象
     */
    private void calculateThroughput(TestResult result) {
        List<Request> requests = readSendLog.getRequests();

        // 每秒处理的request数
        System.out.println("请求总数：" + requests.size());
        result.requestThroughput = (double) requests.size() / ((double) (testEndTime - testStartTime) / 1000);

        // 计算任务总数
        int taskTotalSum = 0;
        for (Request r : requests) {
            taskTotalSum += r.task_sum;
        }
        System.out.println("任务总数：" + taskTotalSum);

        // 计算每秒处理的task数
        result.taskThroughput = (double) taskTotalSum / ((double) (testEndTime - testStartTime) / 1000);

    }

    /**
     * 计算端到端时间
     *
     * @param result 测试结果对象
     */
    private void calculateEndToEndTime(TestResult result) {
        // 保存所有request记录（带有request端到端时间和task端到端时间）
        result.requests = readSendLog.getRequests();
        System.out.println("所有请求：" + readSendLog.getRequests());

        // 保存测试总时间
        System.out.println("测试开始时间" + testStartTime);
        System.out.println("测试结束时间" + testEndTime);
        result.totalTime = testEndTime - testStartTime;
    }

    /**
     * 计算P90延迟
     *
     * @param result 测试结果对象
     */
    private void calculateP90Latency(TestResult result) {
        List<Long> latencies = new ArrayList<>();

        // 收集所有task的延迟
        for (Request request : readSendLog.getRequests()) {
            for (Request.Task task : request.tasks.values()) {
                latencies.add(task.cost_time);
            }
        }

        if (!latencies.isEmpty()) {
            // 排序延迟列表
            Collections.sort(latencies);

            // 计算P90延迟 (90th percentile)
            int index = (int) Math.ceil(0.9 * latencies.size()) - 1;
            if (index < 0) index = 0;
            result.p90Latency = latencies.get(index);
        } else {
            result.p90Latency = 0;
        }

        result.allTaskLatencies = latencies;
    }

    /**
     * 计算每种任务（根据model_id划分）的平均延迟
     *
     * @param result 测试结果对象
     */
    private void calculateAverageLatencyByModel(TestResult result) {
        Map<String, List<Long>> latenciesByModel = new HashMap<>();

        // 按model_id分组收集延迟
        for (Request request : readSendLog.getRequests()) {
            for (Request.Task task : request.tasks.values()) {
                latenciesByModel.computeIfAbsent(task.model_id, k -> new ArrayList<>()).add(task.cost_time);
            }
        }

        // 计算每种model的平均延迟
        result.averageLatencyByModel = getAverageLatency(latenciesByModel);
    }

    private static Map<String, Double> getAverageLatency(Map<String, List<Long>> latenciesByModel) {
        Map<String, Double> averageLatency = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : latenciesByModel.entrySet()) {
            String modelId = entry.getKey();
            List<Long> latencies = entry.getValue();

            if (!latencies.isEmpty()) {
                long sum = 0;
                for (Long latency : latencies) {
                    sum += latency;
                }
                double average = (double) sum / latencies.size();
                averageLatency.put(modelId, average);
            }
        }
        return averageLatency;
    }

    public boolean isTestEnd() {
        GlobalResourceManager instance = GlobalResourceManager.getInstance();
        int requestCount = instance.getRequestCount();
        return solvedRequests >= requestCount;
    }
}

/*
TODO: 多样化可配置的测试输出结果，报告，多样呈现
 */