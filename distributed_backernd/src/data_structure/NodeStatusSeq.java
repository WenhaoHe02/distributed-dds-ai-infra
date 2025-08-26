package data_structure;

import com.zrdds.infrastructure.ZRSequence;

public class NodeStatusSeq extends ZRSequence<NodeStatus> {

    protected Object[] alloc_element(int length) {
        NodeStatus[] result = new NodeStatus[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new NodeStatus();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        NodeStatus typedDst = (NodeStatus)dstEle;
        NodeStatus typedSrc = (NodeStatus)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}