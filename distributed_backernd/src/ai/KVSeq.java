package ai;

import com.zrdds.infrastructure.ZRSequence;

public class KVSeq extends ZRSequence<KV> {

    protected Object[] alloc_element(int length) {
        KV[] result = new KV[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new KV();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        KV typedDst = (KV)dstEle;
        KV typedSrc = (KV)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}