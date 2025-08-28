package data_structure;


public class TaskList{
    public String batch_id = "";// @ID(0)
    public String model_id = "";// @ID(1)
    public String assigned_worker_id = "";// @ID(2)
    public data_structure.TaskSeq tasks = new data_structure.TaskSeq();// @ID(3)

    public TaskList(){

    }

    public TaskList(TaskList other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        TaskList typedSrc = (TaskList)src;
        this.batch_id =  typedSrc.batch_id;
        this.model_id =  typedSrc.model_id;
        this.assigned_worker_id =  typedSrc.assigned_worker_id;
        this.tasks.copy(typedSrc.tasks);
        return this;
    }
}