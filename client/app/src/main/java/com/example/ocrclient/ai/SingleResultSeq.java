package com.example.ocrclient.ai;

import com.zrdds.infrastructure.ZRSequence;

public class SingleResultSeq extends ZRSequence<SingleResult> {

    protected Object[] alloc_element(int length) {
        SingleResult[] result = new SingleResult[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new SingleResult();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        SingleResult typedDst = (SingleResult)dstEle;
        SingleResult typedSrc = (SingleResult)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}