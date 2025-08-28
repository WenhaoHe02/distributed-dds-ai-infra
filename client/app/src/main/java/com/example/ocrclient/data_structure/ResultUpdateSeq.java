package com.example.ocrclient.data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class ResultUpdateSeq extends ZRSequence<ResultUpdate> {

    protected Object[] alloc_element(int length) {
        ResultUpdate[] result = new ResultUpdate[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new ResultUpdate();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        ResultUpdate typedDst = (ResultUpdate)dstEle;
        ResultUpdate typedSrc = (ResultUpdate)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}