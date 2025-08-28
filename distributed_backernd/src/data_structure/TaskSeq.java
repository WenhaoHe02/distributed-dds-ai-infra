package data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class TaskSeq extends ZRSequence<Task> {

    protected Object[] alloc_element(int length) {
        Task[] result = new Task[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new Task();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        Task typedDst = (Task)dstEle;
        Task typedSrc = (Task)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}