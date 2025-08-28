package com.example.ocrclient.data_structure;


public class ResultItem{
    public String task_id = "";// @ID(0)
    public String status = "";// @ID(1)
    public Bytes output_blob = new Bytes();// @ID(2)

    public ResultItem(){

    }

    public ResultItem(ResultItem other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        ResultItem typedSrc = (ResultItem)src;
        this.task_id =  typedSrc.task_id;
        this.status =  typedSrc.status;
        this.output_blob.copy(typedSrc.output_blob);
        return this;
    }
}