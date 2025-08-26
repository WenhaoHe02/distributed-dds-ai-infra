package worker;

import data_structure.SingleResult;
import data_structure.SingleTask;
import data_structure.TaskList;
import data_structure.WorkerResult;

public interface ModelRunner {
    static SingleResult runSingleTask(SingleTask task) {
         return new SingleResult();
    }
    static WorkerResult runBatchedTask(TaskList tasks) {
        return new WorkerResult();
    }
}
