package com.example.ocrclient.ai;


public class WorkerResult{
    public SingleResultSeq results = new SingleResultSeq();// @ID(0)
    public int result_num = 0;// @ID(1)

    public WorkerResult(){

    }

    public WorkerResult(WorkerResult other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        WorkerResult typedSrc = (WorkerResult)src;
        this.results.copy(typedSrc.results);
        this.result_num =  typedSrc.result_num;
        return this;
    }
}