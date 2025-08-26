package data_structure;


public class SingleResult{
    public data_structure.SingleTask task = new data_structure.SingleTask();// @ID(0)
    public String status = "";// @ID(1)
    public String output_type = "";// @ID(2)
    public Bytes output_blob = new Bytes();// @ID(4)

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
        this.output_blob.copy(typedSrc.output_blob);
        return this;
    }
}