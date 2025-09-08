package data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class InferenceRequestSeq extends ZRSequence<InferenceRequest> {

    protected Object[] alloc_element(int length) {
        InferenceRequest[] result = new InferenceRequest[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new InferenceRequest();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        InferenceRequest typedDst = (InferenceRequest)dstEle;
        InferenceRequest typedSrc = (InferenceRequest)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}