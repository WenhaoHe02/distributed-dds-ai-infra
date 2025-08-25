package ai;

import com.zrdds.infrastructure.ZRSequence;

public class TaskListSeq extends ZRSequence<TaskList> {

    protected Object[] alloc_element(int length) {
        TaskList[] result = new TaskList[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new TaskList();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        TaskList typedDst = (TaskList)dstEle;
        TaskList typedSrc = (TaskList)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}