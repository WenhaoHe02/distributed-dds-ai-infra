package data_structure;


public class Task{
    public String request_id = "";// @ID(0)
    public String task_id = "";// @ID(1)
    public String client_id = "";// @ID(2)
    public data_structure.Bytes payload = new data_structure.Bytes();// @ID(3)

    public Task(){

    }

    public Task(Task other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        Task typedSrc = (Task)src;
        this.request_id =  typedSrc.request_id;
        this.task_id =  typedSrc.task_id;
        this.client_id =  typedSrc.client_id;
        this.payload.copy(typedSrc.payload);
        return this;
    }
}