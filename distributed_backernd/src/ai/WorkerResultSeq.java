package ai;

import com.zrdds.infrastructure.ZRSequence;

public class WorkerResultSeq extends ZRSequence<WorkerResult> {

    protected Object[] alloc_element(int length) {
        WorkerResult[] result = new WorkerResult[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new WorkerResult();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        WorkerResult typedDst = (WorkerResult)dstEle;
        WorkerResult typedSrc = (WorkerResult)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}