package ai_train;


public class ClientUpdate{
    public int client_id = 0;// @ID(0)
    public int round_id = 0;// @ID(1)
    public long num_samples = 0;// @ID(2)
    public ai_train.Bytes data = new ai_train.Bytes();// @ID(3)

    public ClientUpdate(){

    }

    public ClientUpdate(ClientUpdate other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        ClientUpdate typedSrc = (ClientUpdate)src;
        this.client_id =  typedSrc.client_id;
        this.round_id =  typedSrc.round_id;
        this.num_samples =  typedSrc.num_samples;
        this.data.copy(typedSrc.data);
        return this;
    }
}