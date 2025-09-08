package data_structure;


public class Grant{
    public String batch_id = "";// @ID(0)
    public String winner_worker_id = "";// @ID(1)

    public Grant(){

    }

    public Grant(Grant other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        Grant typedSrc = (Grant)src;
        this.batch_id =  typedSrc.batch_id;
        this.winner_worker_id =  typedSrc.winner_worker_id;
        return this;
    }
}