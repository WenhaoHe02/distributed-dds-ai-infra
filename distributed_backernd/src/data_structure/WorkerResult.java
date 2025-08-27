package data_structure;


public class WorkerResult{
    public String batch_id = "";// @ID(0)
    public String model_id = "";// @ID(1)
    public String worker_id = "";// @ID(2)
    public data_structure.WorkerTaskResultSeq results = new data_structure.WorkerTaskResultSeq();// @ID(3)

    public WorkerResult(){

    }

    public WorkerResult(WorkerResult other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        WorkerResult typedSrc = (WorkerResult)src;
        this.batch_id =  typedSrc.batch_id;
        this.model_id =  typedSrc.model_id;
        this.worker_id =  typedSrc.worker_id;
        this.results.copy(typedSrc.results);
        return this;
    }
}