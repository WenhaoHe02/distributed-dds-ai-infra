package ai;

import com.zrdds.infrastructure.ZRSequence;

public class PingSeq extends ZRSequence<Ping> {

    protected Object[] alloc_element(int length) {
        Ping[] result = new Ping[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new Ping();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        Ping typedDst = (Ping)dstEle;
        Ping typedSrc = (Ping)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}