package ai;


public class SingleResult{
    public ai.SingleTask task = new ai.SingleTask();// @ID(0)
    public String status = "";// @ID(1)
    public String output_type = "";// @ID(2)
    public int latency_ms = 0;// @ID(3)
    public ai.Bytes output_blob = new ai.Bytes();// @ID(4)

    public SingleResult(){

    }

    public SingleResult(SingleResult other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        SingleResult typedSrc = (SingleResult)src;
        this.task.copy(typedSrc.task);
        this.status =  typedSrc.status;
        this.output_type =  typedSrc.output_type;
        this.latency_ms =  typedSrc.latency_ms;
        this.output_blob.copy(typedSrc.output_blob);
        return this;
    }
}