package ai;


public class TaskList{
    public ai.SingleTaskSeq tasks = new ai.SingleTaskSeq();// @ID(0)
    public int task_num = 0;// @ID(1)
    public String worker_id = "";// @ID(2)
    public ai.KVList meta = new ai.KVList();// @ID(3)

    public TaskList(){

    }

    public TaskList(TaskList other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        TaskList typedSrc = (TaskList)src;
        this.tasks.copy(typedSrc.tasks);
        this.task_num =  typedSrc.task_num;
        this.worker_id =  typedSrc.worker_id;
        this.meta.copy(typedSrc.meta);
        return this;
    }
}