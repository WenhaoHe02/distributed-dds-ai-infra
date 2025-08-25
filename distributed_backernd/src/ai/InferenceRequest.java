package ai;


public class InferenceRequest{
    public String request_id = "";// @ID(0)
    public ai.SingleTaskSeq input_blob = new ai.SingleTaskSeq();// @ID(1)
    public int timeout_ms = 0;// @ID(2)
    public ai.KVList info = new ai.KVList();// @ID(3)

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