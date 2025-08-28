package com.example.ocrclient.data_structure;


public class InferenceRequest{
    public String request_id = "";// @ID(0)
    public SingleTaskSeq tasks = new SingleTaskSeq();// @ID(1)

    public InferenceRequest(){

    }

    public InferenceRequest(InferenceRequest other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        InferenceRequest typedSrc = (InferenceRequest)src;
        this.request_id =  typedSrc.request_id;
        this.tasks.copy(typedSrc.tasks);
        return this;
    }
}