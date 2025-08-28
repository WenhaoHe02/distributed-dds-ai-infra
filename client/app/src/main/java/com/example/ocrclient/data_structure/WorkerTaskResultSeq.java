package com.example.ocrclient.data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class WorkerTaskResultSeq extends ZRSequence<WorkerTaskResult> {

    protected Object[] alloc_element(int length) {
        WorkerTaskResult[] result = new WorkerTaskResult[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new WorkerTaskResult();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        WorkerTaskResult typedDst = (WorkerTaskResult)dstEle;
        WorkerTaskResult typedSrc = (WorkerTaskResult)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}