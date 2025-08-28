package data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class ResultItemSeq extends ZRSequence<ResultItem> {

    protected Object[] alloc_element(int length) {
        ResultItem[] result = new ResultItem[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new ResultItem();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        ResultItem typedDst = (ResultItem)dstEle;
        ResultItem typedSrc = (ResultItem)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}