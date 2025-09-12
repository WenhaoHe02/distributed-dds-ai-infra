package data_structure;


public class WorkerTaskResult{
    public String request_id = "";// @ID(0)
    public String task_id = "";// @ID(1)
    public String client_id = "";// @ID(2)
    public String status = "";// @ID(3)
    public data_structure.Bytes output_blob = new data_structure.Bytes();// @ID(4)
    public com.zrdds.infrastructure.StringSeq texts = new com.zrdds.infrastructure.StringSeq();// @ID(5)

    public WorkerTaskResult(){

    }

    public WorkerTaskResult(WorkerTaskResult other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        WorkerTaskResult typedSrc = (WorkerTaskResult)src;
        this.request_id =  typedSrc.request_id;
        this.task_id =  typedSrc.task_id;
        this.client_id =  typedSrc.client_id;
        this.status =  typedSrc.status;
        this.output_blob.copy(typedSrc.output_blob);
        this.texts.copy(typedSrc.texts);
        return this;
    }
}