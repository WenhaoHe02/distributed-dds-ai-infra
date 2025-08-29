package ai_train;

import com.zrdds.infrastructure.ZRSequence;

public class ModelBlobSeq extends ZRSequence<ModelBlob> {

    protected Object[] alloc_element(int length) {
        ModelBlob[] result = new ModelBlob[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new ModelBlob();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        ModelBlob typedDst = (ModelBlob)dstEle;
        ModelBlob typedSrc = (ModelBlob)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}