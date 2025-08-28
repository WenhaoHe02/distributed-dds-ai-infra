package ai_train;

import com.zrdds.infrastructure.ZRSequence;

public class TrainCmdSeq extends ZRSequence<TrainCmd> {

    protected Object[] alloc_element(int length) {
        TrainCmd[] result = new TrainCmd[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new TrainCmd();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        TrainCmd typedDst = (TrainCmd)dstEle;
        TrainCmd typedSrc = (TrainCmd)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}