package com.example.ocrclient.data_structure;


public class OpenBatch{
    public String batch_id = "";// @ID(0)
    public String model_id = "";// @ID(1)
    public int size = 0;// @ID(2)
    public int create_ts_ms = 0;// @ID(3)

    public OpenBatch(){

    }

    public OpenBatch(OpenBatch other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        OpenBatch typedSrc = (OpenBatch)src;
        this.batch_id =  typedSrc.batch_id;
        this.model_id =  typedSrc.model_id;
        this.size =  typedSrc.size;
        this.create_ts_ms =  typedSrc.create_ts_ms;
        return this;
    }
}