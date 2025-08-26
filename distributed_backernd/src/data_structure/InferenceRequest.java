package data_structure;


public class InferenceRequest{
    public String request_id = "";// @ID(0)
    public data_structure.SingleTaskSeq input_blob = new data_structure.SingleTaskSeq();// @ID(1)
    public int timeout_ms = 0;// @ID(2)
    public data_structure.KVList info = new data_structure.KVList();// @ID(3)

    public InferenceRequest(){

    }

    public InferenceRequest(InferenceRequest other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        InferenceRequest typedSrc = (InferenceRequest)src;
        this.request_id =  typedSrc.request_id;
        this.input_blob.copy(typedSrc.input_blob);
        this.timeout_ms =  typedSrc.timeout_ms;
        this.info.copy(typedSrc.info);
        return this;
    }
}