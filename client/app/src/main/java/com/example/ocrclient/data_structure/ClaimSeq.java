package com.example.ocrclient.data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class ClaimSeq extends ZRSequence<Claim> {

    protected Object[] alloc_element(int length) {
        Claim[] result = new Claim[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new Claim();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        Claim typedDst = (Claim)dstEle;
        Claim typedSrc = (Claim)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}