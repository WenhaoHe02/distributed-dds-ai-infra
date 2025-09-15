#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Optional

import DDS_All as dds  # 你的 pybind11 绑定
from normal_distributed.dist_train_ddp_dgc.dds_barrier_verbose import ddp_barrier_verbose
from  normal_distributed.dist_train_ddp_dgc.zrdds_allgather import ZrddsAllgather

# ---------------- Client_v3 (Python refactor of Java V3) ----------------
class Client_v3:
    # ===== 配置（由 JSON 覆盖） =====
    DOMAIN_ID = 0
    CLIENT_ID = 0
    NUM_CLIENTS = 1
    BATCH_SIZE = 32
    DATA_DIR = ""
    PYTHON_EXE = "python"
    TRAINER_PY = ""

    SPARSE_RATIO = 0.1          # 稀疏率（默认 10%）
    COMM_EVERY = 0              # >0 每N步；=0 每epoch；<0 单包
    SPARSE_FIRST_ROUND = False  # 首轮是否也用稀疏（S4 Δ）
    INIT_MODEL_PATH = ""        # 可选：强制起始模型
    WAIT_MODEL_MS = 8000        # 每轮开训前等待上一轮模型的最长时间（ms）

    # ===== DDS 对象 =====
    def __init__(self):
        self.dp = None
        self.pub = None
        self.sub = None
        self.t_cmd = None
        self.t_upd = None
        self.t_model = None

        self.cmd_reader = None
        self.model_reader = None
        self.upd_writer = None

        self.cmd_listener = None
        self.model_listener = None

        self.last_round = -1

        # 最近聚合模型：保存到磁盘，并记录其轮次
        self.latest_model_dir = Path(os.getcwd()) / "global_model"
        self.latest_model_path = self.latest_model_dir / "latest_model.bin"
        self.latest_model_round = -1

        self._quit_event = threading.Event()

    def wait_for_discovery(self,ag: ZrddsAllgather, world: int, timeout_ms: int = 10000, include_self: bool = True,
                           poll_ms: int = 200):
        """阻塞直到 discovery 匹配完成"""
        deadline = time.time() + timeout_ms / 1000.0
        target = world if include_self else max(0, world - 1)

        def _get_pub_count():
            st = ag.writer.get_publication_matched_status()  # 直接返回 DDS_All.PublicationMatchedStatus
            return int(getattr(st, "current_count", 0))

        def _get_sub_count():
            st = ag.reader.get_subscription_matched_status()
            return int(getattr(st, "current_count", 0))

        last_w = last_r = -1
        while True:
            cw, cr = _get_pub_count(), _get_sub_count()
            if (cw >= target) and (cr >= target):
                print(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}")
                return
            if cw != last_w or cr != last_r:
                print(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}")
                last_w, last_r = cw, cr
            if time.time() >= deadline:
                raise TimeoutError(f"discovery timeout: writer={cw}, reader={cr}, target={target}, world={world}")
            time.sleep(poll_ms / 1000.0)

    # ---------------- 静态入口 ----------------
    @staticmethod
    def main(argv):
        if len(argv) != 1:
            print("Usage: python Client_v3.py <client_v3.conf.json>", file=sys.stderr)
            return

        try:
            Client_v3.load_config(argv[0])
            node = Client_v3()

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

        except Exception:
            import traceback
            traceback.print_exc()

    # ---------------- 配置 ----------------
    @staticmethod
    def load_config(conf_path: str):
        with open(conf_path, "r", encoding="utf-8") as f:
            j = json.load(f)

        Client_v3.DOMAIN_ID = j.get("domain_id")
        Client_v3.CLIENT_ID = j.get("client_id")
        Client_v3.NUM_CLIENTS = j.get("num_clients")
        Client_v3.BATCH_SIZE = j.get("batch_size", 32)
        Client_v3.DATA_DIR = j.get("data_dir")
        Client_v3.PYTHON_EXE = j.get("python_exe", os.environ.get("PYTHON_EXE", "python"))
        Client_v3.TRAINER_PY = j.get("trainer_script")

        Client_v3.SPARSE_RATIO = j.get("sparse_ratio", 0.1)
        Client_v3.COMM_EVERY = j.get("comm_every", 0)
        Client_v3.SPARSE_FIRST_ROUND = j.get("sparse_first_round", False)

        Client_v3.INIT_MODEL_PATH = j.get("init_model_path", "").strip()
        Client_v3.WAIT_MODEL_MS = j.get("wait_model_ms", 8000)

        print(
            f"[Client_v3] cfg: domain={Client_v3.DOMAIN_ID} "
            f"client={Client_v3.CLIENT_ID} "
            f"num_clients={Client_v3.NUM_CLIENTS} "
            f"sparse_ratio={Client_v3.SPARSE_RATIO} "
            f"comm_every={Client_v3.COMM_EVERY} "
            f"wait_model_ms={Client_v3.WAIT_MODEL_MS} "
            f"sparse_first_round={Client_v3.SPARSE_FIRST_ROUND} "
            + (f"init_model_path={Client_v3.INIT_MODEL_PATH}" if Client_v3.INIT_MODEL_PATH else "")
        )

    # ---------------- 启动 ----------------
    def start(self):
        try:
            self.latest_model_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        self.dp = dds.DomainParticipantFactory.get_instance().create_participant(
            self.DOMAIN_ID, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0
        )

        # 注册类型
        dds.register_all_types(self.dp)

        # 注册类型
        dds.register_all_types(self.dp)

        # 通信引擎
        ag = ZrddsAllgather(self.dp, topic="federal_train_scripts/allgather_blob")

        # ---- ★ 在 barrier 之前先确保 discovery 已完成
        self.wait_for_discovery(ag, world=Client_v3.NUM_CLIENTS + 1, timeout_ms=100000, include_self=True)

        ok = ddp_barrier_verbose(ag, group_id='1', rank=Client_v3.CLIENT_ID, world=Client_v3.NUM_CLIENTS + 1,
                                 domain_id=Client_v3.DOMAIN_ID, topic_name="federal_train_scripts/allgather_blob",
                                 min_writer_matches=Client_v3.NUM_CLIENTS + 1,
                                 min_reader_matches=Client_v3.NUM_CLIENTS + 1,
                                 match_timeout_s=150.0, barrier_timeout_s=600.0)
        if not ok:
            raise SystemExit("[barrier] failed; check missing ranks / matching logs")

        # Topics
        self.t_cmd = self.dp.create_topic("federal_train_scripts/train_cmd", "TrainCmd", dds.TOPIC_QOS_DEFAULT, None, 0)
        self.t_upd = self.dp.create_topic("federal_train_scripts/client_update", "ClientUpdate", dds.TOPIC_QOS_DEFAULT, None, 0)
        self.t_model = self.dp.create_topic("federal_train_scripts/model_blob", "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)

        # Pub/Sub
        self.pub = self.dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = self.dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        # Writer QoS
        wq = dds.DataWriterQos()
        self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = 8
        self.upd_writer = self.pub.create_datawriter(self.t_upd, wq, None, 0)
        try:
            self.wait_writer_matched(self.upd_writer, 1, 5000)
        except InterruptedError:
            pass

        # Reader QoS
        rq = dds.DataReaderQos()
        self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        rq.history.depth = 8

        self.cmd_reader = self.sub.create_datareader(self.t_cmd, rq, None, 0)
        try:
            self.wait_reader_matched(self.cmd_reader, 1, 5000)
        except InterruptedError:
            pass

        self.model_reader = self.sub.create_datareader(self.t_model, rq, None, 0)
        try:
            self.wait_reader_matched(self.model_reader, 1, 2000)
        except InterruptedError:
            pass

        # ---- 监听 TrainCmd ----
        client = self

        class TrainCmdListener(dds.DataReaderListener):
            def __init__(self, cli):
                super().__init__()
                self.client = cli

            def on_data_available(self, reader):
                try:
                    samples = dds.TrainCmdSeq()
                    infos = dds.SampleInfoSeq()
                    reader.take(
                        samples,
                        infos,
                        -1,
                        dds.ANY_SAMPLE_STATE,
                        dds.ANY_VIEW_STATE,
                        dds.ANY_INSTANCE_STATE,
                    )
                    for i in range(samples.length()):
                        if infos.get_at(i).valid_data:
                            self.process(reader, samples.get_at(i), infos.get_at(i))
                    reader.return_loan(samples, infos)
                except Exception:
                    import traceback

                    traceback.print_exc()

            def process(self, reader, cmd, info):
                if not info.valid_data or cmd is None:
                    return

                round_id = int(cmd.round_id)
                subset = int(cmd.subset_size)
                epochs = int(cmd.epochs)
                seed = int(cmd.seed)
                lr = float(cmd.lr)

                if round_id <= self.client.last_round:
                    return
                self.client.last_round = round_id

                print(
                    f"[Client_v3] TrainCmd: round={round_id} subset={subset} epochs={epochs} lr={lr} seed={seed}"
                )

                try:
                    # 只有“非首轮 & 非首轮稀疏”才等待上一轮模型
                    first_round_sparse = self.client.SPARSE_FIRST_ROUND and round_id == 1
                    if round_id > 1 and not first_round_sparse:
                        self.client.wait_for_init_model_if_needed(round_id)

                    t0 = int(time.time() * 1000)
                    tr = self.client.run_python_training(
                        client_id=self.client.CLIENT_ID,
                        seed=seed,
                        subset=subset,
                        epochs=epochs,
                        lr=lr,
                        batch_size=self.client.BATCH_SIZE,
                        data_dir=self.client.DATA_DIR,
                        round_id=round_id,
                    )
                    t1 = int(time.time() * 1000)
                    print(f"[Client_v3] local federal_train_scripts+pack cost: {t1 - t0} ms")

                    # 发送
                    if tr["mode"] == "stream":
                        packets: List[bytes] = tr["packets"]
                        total = len(packets)
                        for i, pkt in enumerate(packets):
                            upd = dds.ClientUpdate()
                            upd.client_id = self.client.CLIENT_ID
                            upd.round_id = cmd.round_id
                            # 只在最后一包带 num_samples
                            upd.num_samples = int(tr["num_samples"]) if (i == total - 1) else 0
                            upd.data = pkt  # DDS pybind 支持 bytes
                            self.client.upd_writer.write(upd)
                        print(f"[Client_v3] sent stream packets: {total}")
                    else:
                        upd = dds.ClientUpdate()
                        upd.client_id = self.client.CLIENT_ID
                        upd.round_id = cmd.round_id
                        upd.num_samples = int(tr["num_samples"])
                        upd.data = tr["bytes"]
                        self.client.upd_writer.write(upd)
                        print(f"[Client_v3] sent single update bytes={len(tr['bytes'])}")
                except Exception:
                    import traceback

                    traceback.print_exc()

        self.cmd_listener = TrainCmdListener(self)
        self.cmd_reader.set_listener(self.cmd_listener, dds.StatusKind.DATA_AVAILABLE_STATUS)

        # ---- 监听 ModelBlob：保存 latest_model.bin 并记录轮次 ----
        class ModelBlobListener(dds.DataReaderListener):
            def __init__(self, cli):
                super().__init__()
                self.client = cli

            def on_data_available(self, reader):
                try:
                    samples = dds.ModelBlobSeq()
                    infos = dds.SampleInfoSeq()
                    reader.take(
                        samples,
                        infos,
                        -1,
                        dds.ANY_SAMPLE_STATE,
                        dds.ANY_VIEW_STATE,
                        dds.ANY_INSTANCE_STATE,
                    )
                    for i in range(samples.length()):
                        if infos.get_at(i).valid_data:
                            self.process(reader, samples.get_at(i), infos.get_at(i))
                    reader.return_loan(samples, infos)
                except Exception:
                    import traceback

                    traceback.print_exc()

            def process(self, reader, mb, info):
                if not info.valid_data or mb is None:
                    return
                try:
                    buf = mb.data  # bytes
                    self.client.latest_model_dir.mkdir(parents=True, exist_ok=True)
                    with open(self.client.latest_model_path, "wb") as f:
                        f.write(buf)
                    self.client.latest_model_round = int(mb.round_id)
                    print(
                        f"[Client_v3] ModelBlob: round={mb.round_id} -> {self.client.latest_model_path.resolve()}"
                    )
                except Exception as e:
                    print(f"[Client_v3] failed to save ModelBlob: {e}", file=sys.stderr)

        self.model_listener = ModelBlobListener(self)
        self.model_reader.set_listener(self.model_listener, dds.StatusKind.DATA_AVAILABLE_STATUS)

        print("[Client_v3] started. Waiting for TrainCmd...")

    # ---------------- 等待上一轮模型 ----------------
    def wait_for_init_model_if_needed(self, round_id: int):
        if self.INIT_MODEL_PATH:
            # 用户强制指定了 init model，就不等待
            return
        if round_id <= 1:
            return

        need_round = round_id - 1
        print(f"[Client_v3] waiting global model for prev round = {need_round}")
        max_wait_ms = max(self.WAIT_MODEL_MS, 0)
        start = time.time() * 1000.0

        while True:
            have = self.latest_model_round
            if have >= need_round and self.latest_model_path.exists():
                print(f"[Client_v3] found latest_model.bin for round {need_round}")
                return
            if (time.time() * 1000.0 - start) > max_wait_ms:
                print(
                    f"[Client_v3] WARN: waited {int(time.time()*1000.0 - start)} ms "
                    f"but no model for round {need_round}; start cold."
                )
                return
            time.sleep(0.1)

    # ---------------- 关停 ----------------
    def shutdown(self):
        try:
            if self.cmd_reader is not None:
                self.cmd_reader.set_listener(None, 0)
                self.cmd_listener = None
            if self.model_reader is not None:
                self.model_reader.set_listener(None, 0)
                self.model_listener = None
            if self.dp is not None:
                self.dp.delete_contained_entities()
        except Exception as e:
            print(f"[Client_v3] Error during shutdown: {e}")
        print("[Client_v3] shutdown.")
        self._quit_event.set()

    # ---------------- 训练入口（子进程调用 Python 脚本） ----------------
    def run_python_training(
        self,
        client_id: int,
        seed: int,
        subset: int,
        epochs: int,
        lr: float,
        batch_size: int,
        data_dir: str,
        round_id: int,
    ):
        """
        返回：
        - 流式：{"mode":"stream","num_samples":N,"packets":[bytes,...]}
        - 单包：{"mode":"single","num_samples":N,"bytes":bytes}
        """
        # 本轮是否走稀疏
        use_sparse = (round_id > 1) or (self.SPARSE_FIRST_ROUND and round_id == 1)
        compress_this_round = "fp32_sparse" if use_sparse else "fp32_full"

        # 决定 init_model
        init_path: Optional[Path] = None
        if self.INIT_MODEL_PATH and Path(self.INIT_MODEL_PATH).exists():
            init_path = Path(self.INIT_MODEL_PATH)
        elif self.latest_model_path.exists():
            init_path = self.latest_model_path

        # 流式条件：fp32_sparse 且 COMM_EVERY >= 0
        stream_mode = (compress_this_round == "fp32_sparse") and (self.COMM_EVERY >= 0)

        import subprocess

        cmd: List[str] = [self.PYTHON_EXE, self.TRAINER_PY]

        # 输出位置
        out_dir = None
        out_bin = None
        if stream_mode:
            out_dir = Path(tempfile.mkdtemp(prefix="upd_stream_"))
            cmd += ["--out", str(out_dir)]
        else:
            out_bin = Path(tempfile.mkstemp(prefix="upd_", suffix=".bin")[1])
            cmd += ["--out", str(out_bin)]

        # 训练参数
        cmd += [
            "--client_id",
            str(client_id),
            "--num_clients",
            str(self.NUM_CLIENTS),
            "--seed",
            str(seed),
            "--epochs",
            str(epochs),
            "--lr",
            str(lr),
            "--batch_size",
            str(batch_size),
            "--data_dir",
            str(data_dir),
            "--round",
            str(round_id),
            "--subset",
            str(subset),
            "--compress",
            compress_this_round,
            "--sparse_ratio",
            str(self.SPARSE_RATIO),
            "--comm_every",
            str(self.COMM_EVERY),
            "--state_dir",
            str(Path(data_dir) / f"client_{client_id}_state"),
        ]
        if init_path is not None:
            cmd += ["--init_model", str(init_path.resolve())]
            print(f"[Client_v3] init from model: {init_path.resolve()}")
        else:
            print("[Client_v3] no init model found, cold start this round.")

        # 运行子进程并收集 stdout
        print(f"[Client_v3] spawn trainer: {cmd}")
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
        stdout_lines: List[str] = []
        assert p.stdout is not None
        for line in p.stdout:
            line = line.rstrip("\n")
            print(f"[PY] {line}")
            stdout_lines.append(line)
        code = p.wait()
        if code != 0:
            raise RuntimeError(f"trainer exit={code}")

        num_samples = self._parse_num_samples_from_stdout("\n".join(stdout_lines))

        # 收集结果
        if stream_mode:
            packets = self._collect_s4_packets(Path(out_dir))
            self._safe_delete_dir(Path(out_dir))
            return {"mode": "stream", "num_samples": num_samples, "packets": packets}
        else:
            with open(out_bin, "rb") as f:
                data = f.read()
            try:
                Path(out_bin).unlink(missing_ok=True)
            except Exception:
                pass
            return {"mode": "single", "num_samples": num_samples, "bytes": data}

    # ---------------- 工具方法 ----------------
    @staticmethod
    def _collect_s4_packets(dir_path: Path) -> List[bytes]:
        if not dir_path.exists():
            return []
        # 匹配 00001.s4, 00002.s4 ...
        files = sorted([p for p in dir_path.glob("*.s4")], key=lambda p: p.name)
        out: List[bytes] = []
        for p in files:
            with open(p, "rb") as f:
                out.append(f.read())
        return out

    @staticmethod
    def _safe_delete_dir(dir_path: Path):
        try:
            if not dir_path.exists():
                return
            # 先删子文件再删目录
            for p in sorted(dir_path.rglob("*"), key=lambda x: len(str(x)), reverse=True):
                try:
                    if p.is_file() or p.is_symlink():
                        p.unlink(missing_ok=True)
                    else:
                        p.rmdir()
                except Exception:
                    pass
            try:
                dir_path.rmdir()
            except Exception:
                pass
        except Exception:
            pass

    @staticmethod
    def _parse_num_samples_from_stdout(text: str) -> int:
        """
        宽松解析：抓 stdout 中最后一段 {...} JSON，并读取 num_samples
        建议：训练脚本最后一行打印 {"num_samples": N}
        """
        try:
            l = text.rfind("{")
            r = text.rfind("}")
            if l >= 0 and r > l:
                import json as _json

                obj = _json.loads(text[l : r + 1])
                if "num_samples" in obj:
                    return int(obj["num_samples"])
        except Exception:
            import traceback

            traceback.print_exc()
        return 0

    @staticmethod
    def wait_writer_matched(writer, min_matches: int, timeout_ms: int) -> bool:
        start = time.time() * 1000.0
        st = dds.PublicationMatchedStatus()
        last = -1
        while (time.time() * 1000.0 - start) < timeout_ms:
            try:
                st = writer.get_publication_matched_status()
            except Exception as e:
                print(f"[Client_v3] get_publication_matched_status error: {e}")
                return False
            if st.current_count != last:
                print(f"[Client_v3] updWriter matched: current={st.current_count} total={st.total_count} change={st.current_count_change}")
                last = st.current_count
            if st.current_count >= min_matches:
                return True
            time.sleep(0.1)
        return False

    @staticmethod
    def wait_reader_matched(reader, min_matches: int, timeout_ms: int) -> bool:
        start = time.time() * 1000.0
        st = dds.SubscriptionMatchedStatus()
        last = -1
        while (time.time() * 1000.0 - start) < timeout_ms:
            try:
                st = reader.get_subscription_matched_status()
            except Exception as e:
                print(f"[Client_v3] get_subscription_matched_status error: {e}")
                return False
            if st.current_count != last:
                print(f"[Client_v3] reader matched: current={st.current_count} total={st.total_count} change={st.current_count_change}")
                last = st.current_count
            if st.current_count >= min_matches:
                return True
            time.sleep(0.1)
        return False


# ---------------- CLI ----------------
if __name__ == "__main__":
    Client_v3.main(sys.argv[1:])
