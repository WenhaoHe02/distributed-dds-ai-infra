
import DDS_All as dds
import json
import os
import sys
import tempfile
import time
import threading
import signal
from pathlib import Path
from threading import Event
import argparse


# 从train目录导入dist_train_v2模块
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'train_scripts'))
from dist_train_v2 import train_one_client, load_init_model, Net


class Client_v2:
    # ====== 由配置文件加载 ======
    DOMAIN_ID = 200
    CLIENT_ID = 0
    NUM_CLIENTS = 2
    BATCH_SIZE = 0
    DATA_DIR = ""
    PYTHON_EXE = ""
    TRAINER_PY = ""

    # 压缩/稀疏相关（客户端控制）
    COMPRESS = ""  # "int8_sparse" | "int8" | "fp32"
    INT8_CHUNK = 0  # dense int8 时使用（兼容旧路径）
    SPARSE_K = 0  # 若 >0，使用 top-k
    SPARSE_RATIO = 0.0  # 若 >0，使用比例（0.001=0.1%）

    def __init__(self):
        self.dp = None
        self.pub = None
        self.sub = None
        self.tCmd = None
        self.tUpd = None
        self.tModel = None

        self.cmdReader = None
        self.modelReader = None  # 可选
        self.updWriter = None

        # 保存监听器的引用，防止被垃圾回收
        self.cmdListener = None
        self.modelListener = None

        self.lastRound = -1

        # 新增：保存从 Controller 收到的聚合模型，供下一轮初始化
        # 使用内存中的模型参数替代文件存储
        self.latestModelParams = None
        self.latestModelRound = -1
        self._quit_event = Event()

    @staticmethod
    def main(args):
        if len(args) != 1:
            print("Usage: python Client_v2.py <client_v2.conf.json>", file=sys.stderr)
            return
        
        try:
            Client_v2.loadConfig(args[0])
            node = Client_v2()
            
            # 设置信号处理
            def signal_handler(sig, frame):
                try:
                    node.shutdown()
                finally:
                    pass
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            node.start()
            
            # 等待退出信号
            try:
                while not node._quit_event.is_set():
                    time.sleep(0.1)
            except KeyboardInterrupt:
                node.shutdown()
                
        except Exception as e:
            import traceback
            traceback.print_exc()

    @staticmethod
    def loadConfig(confPath):
        with open(confPath, 'r', encoding='utf-8') as f:
            j = json.load(f)

        Client_v2.DOMAIN_ID = j.get("domain_id")
        Client_v2.CLIENT_ID = j.get("client_id")
        Client_v2.NUM_CLIENTS = j.get("num_clients")
        Client_v2.BATCH_SIZE = j.get("batch_size", 32)
        Client_v2.DATA_DIR = j.get("data_dir")
        Client_v2.PYTHON_EXE = j.get("python_exe", os.environ.get("PYTHON_EXE", "python"))
        Client_v2.TRAINER_PY = j.get("trainer_script")

        Client_v2.COMPRESS = j.get("compress", "fp32")
        Client_v2.INT8_CHUNK = j.get("int8_chunk", 1024)  # dense int8 兼容项
        Client_v2.SPARSE_K = j.get("sparse_k", 0)
        Client_v2.SPARSE_RATIO = j.get("sparse_ratio", 0.001)  # 0.1%

        print(f"[Client_v2] cfg: domain={Client_v2.DOMAIN_ID} "
              f"client={Client_v2.CLIENT_ID} "
              f"num_clients={Client_v2.NUM_CLIENTS} "
              f"compress={Client_v2.COMPRESS} "
              f"sparse_k={Client_v2.SPARSE_K} "
              f"sparse_ratio={Client_v2.SPARSE_RATIO}")

    def start(self):
        try:
            # 不再需要创建latestModelDir目录
            pass
        except Exception:
            pass

        self.dp = dds.DomainParticipantFactory.get_instance().create_participant(
            self.DOMAIN_ID,
            dds.DOMAINPARTICIPANT_QOS_DEFAULT,
            None,
            0
        )

        dds.register_all_types(self.dp)

        self.tCmd = self.dp.create_topic(
            "train_scripts/train_cmd",
            "TrainCmd",
            dds.TOPIC_QOS_DEFAULT,
            None,
            0
        )

        self.tUpd = self.dp.create_topic(
            "train_scripts/client_update",
            "ClientUpdate",
            dds.TOPIC_QOS_DEFAULT,
            None,
            0
        )

        self.tModel = self.dp.create_topic(
            "train_scripts/model_blob",
            "ModelBlob",
            dds.TOPIC_QOS_DEFAULT,
            None,
            0
        )

        self.pub = self.dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = self.dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)
        
        # DataWriter
        wq = dds.DataWriterQos()
        self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = 2
        
        self.updWriter = self.pub.create_datawriter(
            self.tUpd, wq, None,0)
        
        try:
            mw = self.waitWriterMatched(self.updWriter, 1, 5000)
            print(f"[Client_v2] updWriter initially matched={mw}")
        except InterruptedError:
            pass
        
        # DataReader
        rq = dds.DataReaderQos()
        self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        rq.history.depth = 2

        self.cmdReader = self.sub.create_datareader(
            self.tCmd, rq, None, 0)
        
        try:
            mr = self.waitReaderMatched(self.cmdReader, 1, 5000)
            print(f"[Client_v2] cmdReader matched={mr}")
        except InterruptedError:
            pass
        
        self.modelReader = self.sub.create_datareader(
            self.tModel, rq, None, 0)
        
        try:
            mr2 = self.waitReaderMatched(self.modelReader, 1, 2000)
            print(f"[Client_v2] modelReader matched={mr2} (first boot may be false)")
        except InterruptedError:
            pass
        
        # 监听 TrainCmd
        class TrainCmdListener(dds.DataReaderListener):
            def __init__(self, client):
                super().__init__()
                self.client = client
            
            def on_data_available(self, reader):
                try:
                    samples = dds.TrainCmdSeq()
                    infos = dds.SampleInfoSeq()
                    reader.take(samples, infos, -1,
                              dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
                    
                    for i in range(samples.length()):
                        if infos.get_at(i).valid_data:
                            self.process_sample(reader, samples.get_at(i), infos.get_at(i))
                    
                    reader.return_loan(samples, infos)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
            
            def process_sample(self, r, cmd, info):
                if cmd is None or info is None or not info.valid_data:
                    return

                round_id = int(cmd.round_id)
                subset_size = int(cmd.subset_size)
                epochs = int(cmd.epochs)
                lr = cmd.lr
                seed = int(cmd.seed)

                if round_id <= self.client.lastRound:
                    return
                    
                self.client.lastRound = round_id

                print(f"[Client_v2] TrainCmd: round={round_id} "
                      f"subset={subset_size} epochs={epochs} lr={lr} seed={seed}")
                print(f"[Client_v2] Python: {Client_v2.PYTHON_EXE}  Trainer: {Client_v2.TRAINER_PY}")

                try:
                    t0 = int(time.time() * 1000)
                    tr = self.client.runPythonTraining(
                        Client_v2.CLIENT_ID, seed, subset_size, epochs, lr, 
                        Client_v2.BATCH_SIZE, Client_v2.DATA_DIR, round_id)
                    t1 = int(time.time() * 1000)
                    print(f"[Client_v2] local train_scripts+pack cost: {t1 - t0} ms")

                    upd = dds.ClientUpdate()
                    upd.client_id = Client_v2.CLIENT_ID
                    upd.round_id = cmd.round_id
                    upd.num_samples = tr.numSamples  # unsigned long long
                    upd.data = tr.bytes

                    self.client.updWriter.write(upd)
                    print(f"[Client_v2] sent ClientUpdate: round={round_id} "
                          f"n={tr.numSamples} bytes={Client_v2.bytesLen(upd.data)}")
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    
        
        # 保存监听器引用，防止被垃圾回收
        self.cmdListener = TrainCmdListener(self)
        self.cmdReader.set_listener(self.cmdListener, dds.StatusKind.DATA_AVAILABLE_STATUS)

        # 监听 ModelBlob：保存为 latest_model.bin 供下一轮初始化
        class ModelBlobListener(dds.DataReaderListener):
            def __init__(self, client):
                super().__init__()
                self.client = client
            
            def on_data_available(self, reader):
                try:
                    samples = dds.ModelBlobSeq()
                    infos = dds.SampleInfoSeq()
                    reader.take(samples, infos, -1,
                              dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
                    
                    for i in range(samples.length()):
                        if infos.get_at(i).valid_data:
                            self.process_sample(reader, samples.get_at(i), infos.get_at(i))
                    
                    reader.return_loan(samples, infos)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
            
            def process_sample(self, r, mb, info):
                if mb is None or info is None or not info.valid_data:
                    return
                    
                try:
                    # 直接将模型参数保存在内存中，而不是写入文件
                    buf = mb.data
                    self.client.latestModelRound = int(mb.round_id)
                    self.client.latestModelParams = buf
                    print(f"[Client_v2] ModelBlob: round={mb.round_id} "
                          f"bytes={Client_v2.bytesLen(mb.data)} -> saved in memory")
                except Exception as e:
                    print(f"[Client_v2] failed to save ModelBlob: {e}", file=sys.stderr)
                    

        # 保存监听器引用，防止被垃圾回收
        self.modelListener = ModelBlobListener(self)
        self.modelReader.set_listener(self.modelListener, dds.StatusKind.DATA_AVAILABLE_STATUS)

        print("[Client_v2] started. Waiting for TrainCmd...")

    def shutdown(self):
        try:
            # 先清除监听器引用
            if self.cmdReader is not None:
                self.cmdReader.set_listener(None, 0)
                self.cmdListener = None
                
            if self.modelReader is not None:
                self.modelReader.set_listener(None, 0)
                self.modelListener = None
                
            # 然后删除DDS实体
            if self.dp is not None:
                self.dp.delete_contained_entities()
        except Exception as e:
            print(f"[Client_v2] Error during shutdown: {e}")
        print("[Client_v2] shutdown.")
        self._quit_event.set()

    # 直接调用train_one_client函数替代subprocess调用
    def runPythonTraining(self, clientId, seed, subset, epochs, lr, batchSize, dataDir, round_id):
        # 构造类似命令行参数的对象
        args = argparse.Namespace()
        args.client_id = clientId
        args.num_clients = self.NUM_CLIENTS
        args.seed = seed
        args.subset = subset
        args.epochs = epochs
        args.lr = lr
        args.batch_size = batchSize
        args.data_dir = dataDir
        args.round = round_id
        
        # 压缩/稀疏控制参数
        if self.COMPRESS.lower() == "int8_sparse":
            args.compress = "int8_sparse"
            args.sparse_k = self.SPARSE_K
            args.sparse_ratio = self.SPARSE_RATIO
        elif self.COMPRESS.lower() == "int8":
            args.compress = "int8"
            args.chunk = self.INT8_CHUNK
        else:
            args.compress = "fp32"
            
        # DGC 参数
        args.dgc_momentum = 0.9
        args.dgc_clip_norm = 0.0
        args.dgc_mask_momentum = 1
        args.dgc_warmup_rounds = 1
        args.partition = "contig"
        args.state_dir = os.path.join(dataDir, f"client_{clientId}_state")
        
        # 创建临时文件用于输出
        with tempfile.NamedTemporaryFile(prefix="upd_", suffix=".bin", delete=False) as tmp:
            outBin = tmp.name
        args.out = outBin

        # 如果有最新的模型参数，则创建临时文件供初始化使用
        temp_model_path = None
        if self.latestModelParams is not None:
            with tempfile.NamedTemporaryFile(prefix="init_model_", suffix=".bin", delete=False) as tmp:
                temp_model_path = tmp.name
                tmp.write(self.latestModelParams)  # 确保正确写入字节数据
            args.init_model = temp_model_path
            print(f"[Client_v2] init from model in memory")
        else:
            args.init_model = None
            print("[Client_v2] no init model found, cold start this round.")

        # 直接调用train_one_client函数
        num_samples, stats = train_one_client(args)
        
        # 读取生成的更新文件
        with open(outBin, 'rb') as f:
            bytes_data = f.read()
            
        # 清理临时文件
        try:
            os.unlink(outBin)
            if temp_model_path:
                os.unlink(temp_model_path)
        except Exception:
            pass
            
        # 返回结果
        return self.TrainResult(num_samples, bytes_data)


    @staticmethod
    def bytesLen(b):
        return 0 if b is None else len(b)

    @staticmethod
    def bytesToArray(b):
        if b is None:
            return bytearray()
        n = len(b)
        out = bytearray(n)
        out[:] = b[:]
        return out

    class TrainResult:
        def __init__(self, n, b):
            self.numSamples = n
            self.bytes = b

    @staticmethod
    def waitWriterMatched(writer, min_matches, timeout_ms):
        """
        等待 DataWriter 至少匹配到 min_matches 个 DataReader，并打印状态
        
        :param writer: ClientUpdateDataWriter 实例
        :param min_matches: 最小匹配数量
        :param timeout_ms: 超时时间（毫秒）
        :return: 如果匹配成功返回 True，否则返回 False
        """
        import time
        start = time.time() * 1000  # 转换为毫秒
        st = dds.PublicationMatchedStatus()
        last = -1
        
        while (time.time() * 1000 - start) < timeout_ms:
            try:
                st=writer.get_publication_matched_status()
            except Exception as e:
                print(f"[ClientV2] get_publication_matched_status error: {e}")
                return False
                
            if st.current_count != last:
                print(f"[ClientV2] updWriter matched: current={st.current_count} "
                      f"total={st.total_count} change={st.current_count_change}")
                last = st.current_count
                
            if st.current_count >= min_matches:
                return True
                
            time.sleep(0.1)  # 睡眠100毫秒
            
        return False

    @staticmethod
    def waitReaderMatched(reader, min_matches, timeout_ms):
        """
        等待 DataReader 至少匹配到 min_matches 个 DataWriter，并打印状态
        
        :param reader: DataReader 实例
        :param min_matches: 最小匹配数量
        :param timeout_ms: 超时时间（毫秒）
        :return: 如果匹配成功返回 True，否则返回 False
        """
        import time
        start = time.time() * 1000  # 转换为毫秒
        st = dds.SubscriptionMatchedStatus()
        last = -1
        
        while (time.time() * 1000 - start) < timeout_ms:
            try:
                st=reader.get_subscription_matched_status()
            except Exception as e:
                print(f"[ClientV2] get_subscription_matched_status error: {e}")
                return False
                
            if st.current_count != last:
                print(f"[ClientV2] reader matched: current={st.current_count} "
                      f"total={st.total_count} change={st.current_count_change}")
                last = st.current_count
                
            if st.current_count >= min_matches:
                return True
                
            time.sleep(0.1)  # 睡眠100毫秒
            
        return False

    @staticmethod
    def magicOf(b):
        """调试：判断负载魔数"""
        if b is None:
            return "null"
        if len(b) < 4:
            return f"short({len(b)})"
        b0, b1, b2, b3 = b[0], b[1], b[2], b[3]
        if b0 == ord('S') and b1 == ord('8') and b2 == 0 and b3 == 1:
            return "S8/v1"
        if b0 == ord('Q') and b1 == ord('8') and b2 == 0 and b3 == 1:
            return "Q8/v1"
        if len(b) % 4 == 0:
            return "FP32(?)"
        return f"??({b0:02X} {b1:02X} {b2:02X} {b3:02X})"

    @staticmethod
    def hexHead(b):
        """可选：看前 8 字节十六进制"""
        if b is None:
            return "null"
        n = min(8, len(b))
        return ' '.join(f"{b[i]:02X}" for i in range(n))


if __name__ == "__main__":
    Client_v2.main(sys.argv[1:])