package com.example.ocrclient.ai;

import com.zrdds.infrastructure.ZRSequence;

public class KVListSeq extends ZRSequence<KVList> {

    protected Object[] alloc_element(int length) {
        KVList[] result = new KVList[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new KVList();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        KVList typedDst = (KVList)dstEle;
        KVList typedSrc = (KVList)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}