package inner;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO: 修改命名，不够直观
@Data
public class Request {
    public String request_id = "";     // @ID(0)
    public int task_sum;               // @ID(1) task总数
    public Map<String, Task> tasks = new HashMap<>(); // @ID(2)
    public String client_id = "";      // @ID(3)

    public Integer cur_total = 0; // 当前已处理的任务数
    public Boolean is_done = false; // 请求是否完成
    public long total_time = 0; // 请求处理完成的耗时

    public Request() {
    }

    /**
     * 深拷贝
     * @param other
     */
    public Request(Request other) {
        this.request_id = other.request_id;
        this.task_sum = other.task_sum;
        this.tasks = new HashMap<>();
        for (Map.Entry<String, Task> entry : other.tasks.entrySet()) {
            this.tasks.put(entry.getKey(), new Task(entry.getValue()));
        }
        this.client_id = other.client_id;
        this.cur_total = other.cur_total;
        this.is_done = other.is_done;
        this.total_time = other.total_time;
    }

    /**
     * 接收一个SendLog对象，将其中的数据填充到本对象中
     */
    public Request(SendLog other) {
        this.request_id = other.request_id;
        this.task_sum = other.task_sum;
        this.client_id = other.client_id;
        this.tasks = new HashMap<>();
        for (SendLog.SingleTask task : other.tasks) {
            this.tasks.put(task.task_id, new Task(task));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Task { // @ID(0)
        public String task_id = "";
        public String model_id = "";   // @ID(1)
        public long send_time;         // @ID(2)

        public long finish_time = 0; // 完成时间戳（ms）
        public long cost_time = 0; // 耗时（ms）

        // 添加拷贝构造函数
        public Task(Task other) {
            this.task_id = other.task_id;
            this.model_id = other.model_id;
            this.send_time = other.send_time;
            this.finish_time = other.finish_time;
            this.cost_time = other.cost_time;
        }

        /**
         * 从SingleTask对象初始化
         * @param other
         */
        public Task(SendLog.SingleTask other) {
            this.task_id = other.task_id;
            this.model_id = other.model_id;
            this.send_time = other.send_time;
        }
    }

    /**
     * tasks 序列化的时候map转list
     * （只在序列化的时候使用）
     * @return
     */
    @JsonProperty("tasks")
    public List<Task> getTaskList() {
        return new ArrayList<>(tasks.values());
    }
}