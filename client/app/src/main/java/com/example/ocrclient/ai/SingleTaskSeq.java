package com.example.ocrclient.ai;

import com.zrdds.infrastructure.ZRSequence;

public class SingleTaskSeq extends ZRSequence<SingleTask> {

    protected Object[] alloc_element(int length) {
        SingleTask[] result = new SingleTask[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new SingleTask();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        SingleTask typedDst = (SingleTask)dstEle;
        SingleTask typedSrc = (SingleTask)srcEle;
        return typedDst.copy(typedSrc);
    }

    /**
     * 添加一个SingleTask到序列末尾
     * @param task 要添加的SingleTask对象
     */
    public void add(SingleTask task) {
        int currentLength = this.length();
        this.ensure_length(currentLength + 1, currentLength + 1);
        this.set_at(currentLength, task);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}