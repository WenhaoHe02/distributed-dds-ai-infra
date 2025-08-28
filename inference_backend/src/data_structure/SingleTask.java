package data_structure;


public class SingleTask{
    public String request_id = "";// @ID(0)
    public String task_id = "";// @ID(1)
    public String model_id = "";// @ID(2)
    public String client_id = "";// @ID(3)
    public data_structure.Bytes payload = new data_structure.Bytes();// @ID(4)

    public SingleTask(){

    }

    public SingleTask(SingleTask other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        SingleTask typedSrc = (SingleTask)src;
        this.request_id =  typedSrc.request_id;
        this.task_id =  typedSrc.task_id;
        this.model_id =  typedSrc.model_id;
        this.client_id =  typedSrc.client_id;
        this.payload.copy(typedSrc.payload);
        return this;
    }
}