package com.example.ocrclient.data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class BytesSeq extends ZRSequence<Bytes> {

    protected Object[] alloc_element(int length) {
        Bytes[] result = new Bytes[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new Bytes();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        Bytes typedDst = (Bytes)dstEle;
        Bytes typedSrc = (Bytes)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}