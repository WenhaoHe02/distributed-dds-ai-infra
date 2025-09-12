package com.example.ocrclient.data_structure;


public class Bytes{
    public com.zrdds.infrastructure.ByteSeq value = new com.zrdds.infrastructure.ByteSeq();// @ID(0)

    public Bytes(){

    }

    public Bytes(Bytes other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        Bytes typedSrc = (Bytes)src;
        this.value.copy(typedSrc.value);
        return this;
    }
}