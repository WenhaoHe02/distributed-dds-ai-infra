package data_structure;


public class KVList{
    public data_structure.KVSeq value = new data_structure.KVSeq();// @ID(0)

    public KVList(){

    }

    public KVList(KVList other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        KVList typedSrc = (KVList)src;
        this.value.copy(typedSrc.value);
        return this;
    }
}