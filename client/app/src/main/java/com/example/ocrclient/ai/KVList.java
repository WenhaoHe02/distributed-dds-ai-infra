package com.example.ocrclient.ai;


public class KVList{
    public KVSeq value = new KVSeq();// @ID(0)

    public KVList(){

    }

    public KVList(KVList other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        KVList typedSrc = (KVList)src;
        this.value.copy(typedSrc.value);
        return this;
    }
}