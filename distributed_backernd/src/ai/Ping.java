package ai;


public class Ping{
    public String msg = "";// @ID(0)

    public Ping(){

    }

    public Ping(Ping other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        Ping typedSrc = (Ping)src;
        this.msg =  typedSrc.msg;
        return this;
    }
}