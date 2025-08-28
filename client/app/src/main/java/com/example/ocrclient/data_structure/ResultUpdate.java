package com.example.ocrclient.data_structure;


public class ResultUpdate{
    public String request_id = "";// @ID(0)
    public String client_id = "";// @ID(1)
    public ResultItemSeq items = new ResultItemSeq();// @ID(2)

    public ResultUpdate(){

    }

    public ResultUpdate(ResultUpdate other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        ResultUpdate typedSrc = (ResultUpdate)src;
        this.request_id =  typedSrc.request_id;
        this.client_id =  typedSrc.client_id;
        this.items.copy(typedSrc.items);
        return this;
    }
}