package ai_train;


public class ModelBlob{
    public int round_id = 0;// @ID(0)
    public ai_train.Bytes data = new ai_train.Bytes();// @ID(1)

    public ModelBlob(){

    }

    public ModelBlob(ModelBlob other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        ModelBlob typedSrc = (ModelBlob)src;
        this.round_id =  typedSrc.round_id;
        this.data.copy(typedSrc.data);
        return this;
    }
}