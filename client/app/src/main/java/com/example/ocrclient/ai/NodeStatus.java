package com.example.ocrclient.ai;


public class NodeStatus{
    public String worker_id = "";// @ID(0)
    public String model_id = "";// @ID(1)
    public String host = "";// @ID(2)
    public int queue_depth = 0;// @ID(3)
    public int est_capacity = 0;// @ID(4)
    public String health = "";// @ID(5)
    public int heartbeat_ms = 0;// @ID(6)

    public NodeStatus(){

    }

    public NodeStatus(NodeStatus other){
        this();
        copy(other);
    }

    public Object copy(Object src) {
        NodeStatus typedSrc = (NodeStatus)src;
        this.worker_id =  typedSrc.worker_id;
        this.model_id =  typedSrc.model_id;
        this.host =  typedSrc.host;
        this.queue_depth =  typedSrc.queue_depth;
        this.est_capacity =  typedSrc.est_capacity;
        this.health =  typedSrc.health;
        this.heartbeat_ms =  typedSrc.heartbeat_ms;
        return this;
    }
}