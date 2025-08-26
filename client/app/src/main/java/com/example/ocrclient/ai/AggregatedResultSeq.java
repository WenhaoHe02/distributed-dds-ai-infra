package com.example.ocrclient.ai;

import com.zrdds.infrastructure.ZRSequence;

public class AggregatedResultSeq extends ZRSequence<AggregatedResult> {

    protected Object[] alloc_element(int length) {
        AggregatedResult[] result = new AggregatedResult[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new AggregatedResult();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        AggregatedResult typedDst = (AggregatedResult)dstEle;
        AggregatedResult typedSrc = (AggregatedResult)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}