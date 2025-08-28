package com.example.ocrclient.data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class GrantSeq extends ZRSequence<Grant> {

    protected Object[] alloc_element(int length) {
        Grant[] result = new Grant[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new Grant();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        Grant typedDst = (Grant)dstEle;
        Grant typedSrc = (Grant)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}