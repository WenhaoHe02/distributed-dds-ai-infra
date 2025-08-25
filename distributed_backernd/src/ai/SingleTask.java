package ai;


public class SingleTask{
    public ai.Bytes input_blob = new ai.Bytes();// @ID(0)
    public String request_id = "";// @ID(1)
    public String model_id = "";// @ID(2)
    public String client_id = "";// @ID(3)
    public String task_id = "";// @ID(4)

    public SingleTask(){

    }

    public SingleTask(SingleTask other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        SingleTask typedSrc = (SingleTask)src;
        this.input_blob.copy(typedSrc.input_blob);
        this.request_id =  typedSrc.request_id;
        this.model_id =  typedSrc.model_id;
        this.client_id =  typedSrc.client_id;
        this.task_id =  typedSrc.task_id;
        return this;
    }
}