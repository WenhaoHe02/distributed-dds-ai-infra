package data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class OpenBatchSeq extends ZRSequence<OpenBatch> {

    protected Object[] alloc_element(int length) {
        OpenBatch[] result = new OpenBatch[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new OpenBatch();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        OpenBatch typedDst = (OpenBatch)dstEle;
        OpenBatch typedSrc = (OpenBatch)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}