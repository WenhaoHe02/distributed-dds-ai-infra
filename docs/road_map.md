# 基于DDS的ai推理优化系统需求分析
## v1
- 实现 **client->patcher->worker->client-> spliter->client**的通路
### patcher
- client 发送多个基于不同模型的推理请求，patcher对每5ms到达的多个请求拆散后按照model_id打包成TaskList（0为OCR任务，1为图像识别任务）
- 一个TaskList代表一个batch处理的任务，因此patcher决定batch大小
### worker
- 只考虑CPU运行环境
- 在Java中运行Woker节点，通过Java进程运行推理脚本
- 每个worker只处理一种模型的推理，一种模型可能有多个worker instance，worker根据自身负载情况和模型种类订阅TaskList
- worker 按batch处理所有的TaskList，用结果构造WokerResult
  
### spliter
- 订阅WorkerResult，把一个WorkerResult中相同request id的task分在一起构造Result，并进行发布，client端根据request_id订阅还没过期的请求
（是否过期由client决定） 