package data_structure;


public class Claim{
    public String batch_id = "";// @ID(0)
    public String worker_id = "";// @ID(1)
    public int queue_length = 0;// @ID(2)

    public Claim(){

    }

    public Claim(Claim other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        Claim typedSrc = (Claim)src;
        this.batch_id =  typedSrc.batch_id;
        this.worker_id =  typedSrc.worker_id;
        this.queue_length =  typedSrc.queue_length;
        return this;
    }
}