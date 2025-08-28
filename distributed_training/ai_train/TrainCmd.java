package ai_train;


public class TrainCmd{
    public int round_id = 0;// @ID(0)
    public int subset_size = 0;// @ID(1)
    public int epochs = 0;// @ID(2)
    public double lr = 0;// @ID(3)
    public int seed = 0;// @ID(4)

    public TrainCmd(){

    }

    public TrainCmd(TrainCmd other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        TrainCmd typedSrc = (TrainCmd)src;
        this.round_id =  typedSrc.round_id;
        this.subset_size =  typedSrc.subset_size;
        this.epochs =  typedSrc.epochs;
        this.lr =  typedSrc.lr;
        this.seed =  typedSrc.seed;
        return this;
    }
}