# components of end to end system（v1,简化版）
## Client
### 数据类型
发布

订阅

### 功能

## Dispatcher
### 数据类型
发布
```
public class SingleTask{
    public data_structure.Bytes input_blob = new data_structure.Bytes();
    public String request_id = "";
    public String model_id = "";
    public String client_id = "";
    public String task_id = "";
}
```
订阅
```
public class InferenceRequest{
    public String request_id = "";
    public data_structure.SingleTaskSeq input_blob = new data_structure.SingleTaskSeq();
    public int timeout_ms = 0;
    public data_structure.KVList info = new data_structure.KVList();
}
```
```
public class SingleResult{
    public data_structure.SingleTask task = new data_structure.SingleTask();
    public String status = "";
    public String output_type = "";
    public long latency_ms = 0;
    public Bytes output_blob = new Bytes();
}
```
### 功能
可接受来自client的请求并返回空结果，可向Worker发送SingleTask，接收并打印Woker返回的结果