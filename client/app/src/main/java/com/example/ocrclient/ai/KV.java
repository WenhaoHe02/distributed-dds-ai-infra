package com.example.ocrclient.ai;


public class KV{
    public String key = "";// @ID(0)
    public String value = "";// @ID(1)

    public KV(){

    }

    public KV(KV other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        KV typedSrc = (KV)src;
        this.key =  typedSrc.key;
        this.value =  typedSrc.value;
        return this;
    }
}