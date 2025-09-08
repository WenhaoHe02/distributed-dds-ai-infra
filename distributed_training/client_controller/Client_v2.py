# 从ai_train模块导入所需类型 (TrainCmd, ClientUpdate, ModelBlob, Bytes等)
import DDS_All as dds

# DDS相关导入
# from com.zrdds.domain import DomainParticipant, DomainParticipantFactory
# from com.zrdds.infrastructure import StatusKind, ReturnCode_t, InstanceHandle_t, PublicationMatchedStatus, SubscriptionMatchedStatus
# from com.zrdds.publication import Publisher, DataWriterQos
# from com.zrdds.subscription import DataReader, SimpleDataReaderListener, Subscriber, DataReaderQos
# from com.zrdds.topic import Topic

# 不再需要自定义SimpleDataReaderListener基类，直接使用DDS_All提供的DataReaderListener

# 标准库导入
import json
import os
import sys
import subprocess
import tempfile
import time
import threading
import signal
from pathlib import Path
from typing import List, Optional, Dict, Any
from threading import Event


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
        self.latestModelDir = Path(os.getcwd()) / "global_model"
        self.latestModelPath = self.latestModelDir / "latest_model.bin"
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
            os.makedirs(self.latestModelDir, exist_ok=True)
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
            "train/train_cmd",
            "TrainCmd",
            dds.TOPIC_QOS_DEFAULT,
            None,
            0
        )

        self.tUpd = self.dp.create_topic(
            "train/client_update",
            "ClientUpdate",
            dds.TOPIC_QOS_DEFAULT,
            None,
            0
        )

        self.tModel = self.dp.create_topic(
            "train/model_blob",
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
                    print(f"[Client_v2] local train+pack cost: {t1 - t0} ms")

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
                    buf = Client_v2.bytesToArray(mb.data)
                    with open(self.client.latestModelPath, 'wb') as f:
                        f.write(buf)
                    self.client.latestModelRound = int(mb.round_id)
                    print(f"[Client_v2] ModelBlob: round={mb.round_id} "
                          f"bytes={Client_v2.bytesLen(mb.data)} -> saved to {self.client.latestModelPath}")
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

    # 调 Python：stdout 打 JSON（含 num_samples）；二进制写临时文件并读回
    def runPythonTraining(self, clientId, seed, subset, epochs, lr, batchSize, dataDir, round_id):
        with tempfile.NamedTemporaryFile(prefix="upd_", suffix=".bin", delete=False) as tmp:
            outBin = tmp.name

        cmd = []
        cmd.append(self.PYTHON_EXE)
        cmd.append(self.TRAINER_PY)

        cmd.append("--client_id")
        cmd.append(str(clientId))
        cmd.append("--num_clients")
        cmd.append(str(self.NUM_CLIENTS))
        cmd.append("--seed")
        cmd.append(str(seed))
        if subset > 0:
            cmd.append("--subset")
            cmd.append(str(subset))
        cmd.append("--epochs")
        cmd.append(str(epochs))
        cmd.append("--lr")
        cmd.append(str(lr))
        cmd.append("--batch_size")
        cmd.append(str(batchSize))
        cmd.append("--data_dir")
        cmd.append(dataDir)
        cmd.append("--round")
        cmd.append(str(round_id))

        # 压缩/稀疏控制参数
        if self.COMPRESS.lower() == "int8_sparse":
            cmd.append("--compress")
            cmd.append("int8_sparse")
            if self.SPARSE_K > 0:
                cmd.append("--sparse_k")
                cmd.append(str(self.SPARSE_K))
            elif self.SPARSE_RATIO > 0.0:
                cmd.append("--sparse_ratio")
                cmd.append(str(self.SPARSE_RATIO))
        elif self.COMPRESS.lower() == "int8":
            cmd.append("--compress")
            cmd.append("int8")
            cmd.append("--chunk")
            cmd.append(str(self.INT8_CHUNK))
        else:
            cmd.append("--compress")
            cmd.append("fp32")

        # DGC 参数（可按需改为配置读取）
        cmd.append("--dgc_momentum")
        cmd.append("0.9")
        cmd.append("--dgc_clip_norm")
        cmd.append("0.0")
        cmd.append("--dgc_mask_momentum")
        cmd.append("1")
        cmd.append("--dgc_warmup_rounds")
        cmd.append("1")

        # === 新增：如存在最新聚合模型，则传给 Python 作为初始化 ===
        if os.path.exists(self.latestModelPath):
            cmd.append("--init_model")
            cmd.append(str(self.latestModelPath))
            print(f"[Client_v2] init from model: {self.latestModelPath}")
        else:
            print("[Client_v2] no init model found, cold start this round.")

        cmd.append("--out")
        cmd.append(outBin)
        stateDir = os.path.join(dataDir, f"client_{clientId}_state")
        cmd.append("--state_dir")
        cmd.append(stateDir)

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                 text=True, encoding='utf-8')

        sb = []
        for line in process.stdout:
            print(f"[PY] {line.rstrip()}")
            sb.append(line)
        
        code = process.wait()
        if code != 0:
            raise RuntimeError(f"trainer exit={code}")

        numSamples = self.parseNumSamplesFromJson(''.join(sb))
        with open(outBin, 'rb') as f:
            bytes_data = f.read()

        # 清理输出文件
        try:
            os.unlink(outBin)
        except Exception:
            pass

        return self.TrainResult(numSamples, bytes_data)

    @staticmethod
    def parseNumSamplesFromJson(text):
        try:
            l = text.find('{')
            r = text.rfind('}')
            if l >= 0 and r > l:
                json_str = text[l:r+1]
                obj = json.loads(json_str)
                if "num_samples" in obj:
                    return obj["num_samples"]
        except Exception as e:
            import traceback
            traceback.print_exc()
        return 0

    # @staticmethod
    # def toBytes(raw):
    #     out = Bytes()
    #     if raw is not None:
    #         out.loan_contiguous(raw, len(raw), len(raw))
    #     return out

    @staticmethod
    def bytesLen(b):
        return 0 if b is None else len(b)

    @staticmethod
    def bytesToArray(b):
        if b is None:
            return bytearray()
        n = len(b)
        out = bytearray(n)
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