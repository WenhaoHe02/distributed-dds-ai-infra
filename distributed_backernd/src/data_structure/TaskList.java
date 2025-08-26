package data_structure;


public class TaskList{
    public data_structure.SingleTaskSeq tasks = new data_structure.SingleTaskSeq();// @ID(0)
    public int task_num = 0;// @ID(1)
    public String worker_id = "";// @ID(2)
    public data_structure.KVList meta = new data_structure.KVList();// @ID(3)

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