package data_structure;


public class AggregatedResult{
    public data_structure.SingleResultSeq results = new data_structure.SingleResultSeq();// @ID(0)
    public String client_id = "";// @ID(1)
    public String request_id = "";// @ID(2)
    public String status = "";// @ID(3)
    public String error_message = "";// @ID(4)

    public AggregatedResult(){

    }

    public AggregatedResult(AggregatedResult other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        AggregatedResult typedSrc = (AggregatedResult)src;
        this.results.copy(typedSrc.results);
        this.client_id =  typedSrc.client_id;
        this.request_id =  typedSrc.request_id;
        this.status =  typedSrc.status;
        this.error_message =  typedSrc.error_message;
        return this;
    }
}