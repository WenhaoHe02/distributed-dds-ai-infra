package ai_train;

import com.zrdds.infrastructure.ZRSequence;

public class ClientUpdateSeq extends ZRSequence<ClientUpdate> {

    protected Object[] alloc_element(int length) {
        ClientUpdate[] result = new ClientUpdate[length];
        for (int i = 0; i < result.length; ++i) {
             result[i] = new ClientUpdate();
        }
        return result;
    }

    protected Object copy_from_element(Object dstEle, Object srcEle){
        ClientUpdate typedDst = (ClientUpdate)dstEle;
        ClientUpdate typedSrc = (ClientUpdate)srcEle;
        return typedDst.copy(typedSrc);
    }

    public void pull_from_nativeI(long nativeSeq){

    }

    public void push_to_nativeI(long nativeSeq){

    }
}