package inner;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 发送日志结构
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SendLog {
    public String request_id = "";     // @ID(0)
    public int task_sum;               // @ID(1)
    public List<SingleTask> tasks = new ArrayList<>(); // @ID(2)
    public String client_id = "";      // @ID(3)

    public SendLog(SendLog other) {
        copy(other);
    }

    public Object copy(Object src) {
        SendLog typedSrc = (SendLog) src;
        this.request_id = typedSrc.request_id;
        this.task_sum = typedSrc.task_sum;
        this.client_id = typedSrc.client_id;
        
        // 深拷贝tasks列表
        this.tasks.clear();
        for (SingleTask task : typedSrc.tasks) {
            this.tasks.add(new SingleTask(task));
        }
        return this;
    }
    
    // 内部类Task用于表示tasks数组中的元素
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SingleTask {
        public String task_id = "";    // @ID(0)
        public String model_id = "";   // @ID(1)
        public long send_time;         // @ID(2)
        
        public SingleTask(SingleTask other) {
            this.task_id = other.task_id;
            this.model_id = other.model_id;
            this.send_time = other.send_time;
        }
    }
}