#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import signal
import sys
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import DDS_All as dds
from cc_dds_barrier import *

TOPIC_TRAIN_CMD     = "train_scripts/train_cmd"
TOPIC_CLIENT_UPDATE = "train_scripts/client_update"
TOPIC_MODEL_BLOB    = "train_scripts/model_blob"


class ControllerV4:
    GROUP = os.environ.get("GROUP_ID", "train-20250908-01")
    # ========== 配置 ==========
    class Config:
        def __init__(self):
            self.domain_id: int = 0
            self.expected_clients: int = 0
            self.timeout_ms: int = 0
            self.rounds: int = 1
            self.python_exe: str = "python"
            self.eval_script: str = ""
            self.data_dir: str = ""
            self.batch_size: int = 64

    def __init__(self, cfg: "ControllerV4.Config"):
        self.cfg = cfg

        # DDS
        self.dp = None
        self.publisher = None
        self.subscriber = None
        self.train_cmd_writer = None
        self.model_blob_writer = None
        self.client_update_reader = None

        # 轮次（Java 版从 2 开始）
        self._round_counter = 2

        # 全局模型
        self.current_model: Optional[List[float]] = None
        self.model_dim: int = -1

        # round -> client_id -> ClientStream
        self.round_streams: Dict[int, Dict[int, "ClientStream"]] = {}
        self._lock = threading.Lock()

        self._quit = threading.Event()

    # ========== 运行入口 ==========
    @staticmethod
    def main(argv: List[str]):
        if len(argv) != 1:
            print("Usage: python Controller_v3.py <controller_v3.conf.json>", file=sys.stderr)
            return

        cfg = ControllerV4._load_config(argv[0])
        ctrl = ControllerV4(cfg)

        def _on_signal(sig, frame):
            ctrl.shutdown()

        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)

        ctrl.init()

        try:
            for i in range(cfg.rounds):
                # 与 Java 示例一致：subset=60000, epochs=5, lr=0.01, seed=12345+i
                ctrl.run_training_round(subset_size=60000, epochs=5, lr=0.01, seed=12345 + i)
        finally:
            try:
                ctrl.shutdown()
            except Exception:
                pass

    @staticmethod
    def _load_config(conf_path: str) -> "ControllerV3.Config":
        j = json.loads(Path(conf_path).read_text(encoding="utf-8"))
        cfg = ControllerV4.Config()
        cfg.domain_id = int(j["domain_id"])
        cfg.expected_clients = int(j["expected_clients"])
        cfg.timeout_ms = int(j["timeout_ms"])
        cfg.rounds = int(j.get("rounds", 1))
        cfg.python_exe = j["python_exe"]
        cfg.eval_script = j["eval_script"]
        cfg.data_dir = j["data_dir"]
        cfg.batch_size = int(j.get("batch_size", 64))

        print(
            f"[Controller_v3] cfg: domain={cfg.domain_id} expected_clients={cfg.expected_clients} "
            f"timeout_ms={cfg.timeout_ms} rounds={cfg.rounds}"
        )
        return cfg

    # ========== DDS 初始化 ==========
    def init(self):
        dpf = dds.DomainParticipantFactory.get_instance()
        self.dp = dpf.create_participant(
            self.cfg.domain_id, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0
        )
        if not self.dp:
            raise RuntimeError("create participant failed")

        dds.register_all_types(self.dp)

        # 注册类型
        dds.register_all_types(self.dp)

        allgather, conn_manager = create_barrier_system(
            node_role=NodeRole.CONTROLLER,
            node_id=0,  # controller通常是id=0
            world_size=self.cfg.expected_clients,  # 1个controller + 3个client
            domain_participant=self.dp,
            type=NodeRole.CONTROLLER,
            debug=True
        )

        # 等待所有连接
        if conn_manager.wait_for_all_connections(timeout_s=600):
            print("All nodes connected, ready for training!")
        else:
            print("Connection failed!")

        t_cmd = self.dp.create_topic(TOPIC_TRAIN_CMD, "TrainCmd", dds.TOPIC_QOS_DEFAULT, None, 0)
        t_upd = self.dp.create_topic(TOPIC_CLIENT_UPDATE, "ClientUpdate", dds.TOPIC_QOS_DEFAULT, None, 0)
        t_mdl = self.dp.create_topic(TOPIC_MODEL_BLOB, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)

        self.publisher = self.dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.subscriber = self.dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        # writers
        wq = dds.DataWriterQos()
        self.publisher.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = 8
        self.train_cmd_writer = self.publisher.create_datawriter(t_cmd, wq, None, 0)
        self.model_blob_writer = self.publisher.create_datawriter(t_mdl, wq, None, 0)
        if not self.train_cmd_writer or not self.model_blob_writer:
            raise RuntimeError("create writer failed")

        self._wait_writer_matched(self.train_cmd_writer, 1, 5000)
        self._wait_writer_matched(self.model_blob_writer, 1, 5000)

        # reader
        rq = dds.DataReaderQos()
        self.subscriber.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        rq.history.depth = 64

        self.client_update_reader = self.subscriber.create_datareader(
            t_upd, rq, None, 0
        )
        if not self.client_update_reader:
            raise RuntimeError("create ClientUpdate reader failed")

        # 监听器
        class ClientUpdateListener(dds.DataReaderListener):
            def __init__(self, outer: "ControllerV3"):
                super().__init__()
                self.outer = outer

            def on_data_available(self, reader):
                try:
                    samples = dds.ClientUpdateSeq()
                    infos = dds.SampleInfoSeq()
                    reader.take(
                        samples, infos, -1,
                        dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE
                    )
                    try:
                        for i in range(samples.length()):
                            if not infos.get_at(i).valid_data:
                                continue
                            cu = samples.get_at(i)
                            self.outer._on_update(cu)
                        # end for
                    finally:
                        reader.return_loan(samples, infos)
                except Exception:
                    import traceback
                    traceback.print_exc()

        self.client_update_reader.set_listener(
            ClientUpdateListener(self), dds.StatusKind.DATA_AVAILABLE_STATUS
        )

        self._wait_reader_matched(self.client_update_reader, 1, 5000)
        print("[Controller_v3] ready.")

    # ========== 更新回调 ==========
    class ClientStream:
        __slots__ = ("packets", "num_samples", "final_received", "lock")

        def __init__(self):
            self.packets: List[bytes] = []
            self.num_samples: int = 0    # 仅最后一包>0
            self.final_received: bool = False
            self.lock = threading.Lock()

        def add_packet(self, data: bytes, ns: int):
            with self.lock:
                self.packets.append(data)
                if ns and ns > 0:
                    self.final_received = True
                    self.num_samples = int(ns)

    ClientStream = ClientStream  # alias for typing

    def _on_update(self, cu):
        payload: bytes = cu.data
        rnd = int(cu.round_id)
        cid = int(cu.client_id)
        ns = int(cu.num_samples)

        with self._lock:
            m = self.round_streams.setdefault(rnd, {})
            cs = m.setdefault(cid, ControllerV4.ClientStream())
            cs.add_packet(payload, ns)

        print(
            f"[Controller_v3] recv update: round={rnd} client={cid} "
            f"bytes={0 if payload is None else len(payload)} "
            f"ns={ns} magic={self._magic_of(payload)}"
        )

    # ========== 单轮调度 ==========
    def run_training_round(self, subset_size: int, epochs: int, lr: float, seed: int):
        round_id = self._round_counter
        self._round_counter += 1

        print(f"\n================ Round {round_id} ================")
        t0 = int(time.time() * 1000)

        cmd = dds.TrainCmd()
        cmd.round_id = int(round_id)
        cmd.subset_size = int(subset_size)
        cmd.epochs = int(epochs)
        cmd.lr = float(lr)
        cmd.seed = int(seed)
        self.train_cmd_writer.write(cmd)
        print(f"[Controller_v3] TrainCmd written: round={round_id}")

        streams = self._wait_for_round_streams(round_id, self.cfg.expected_clients, self.cfg.timeout_ms)
        if not streams:
            print("[Controller_v3] no updates collected, skip this round.", file=sys.stderr)
            return

        print(f"[Controller_v3] collected clients: {len(streams)} / expected {self.cfg.expected_clients}")

        cvs = self._collect_client_vectors(streams)
        self._apply_and_publish(cvs, round_id)

        t1 = int(time.time() * 1000)
        print(f"[Controller_v3] round time: {t1 - t0} ms")

        if self.current_model is not None:
            model_bytes = self._float32_to_bytes_le(self.current_model)
            self._evaluate_model(model_bytes)

        # 清理该轮的流
        with self._lock:
            self.round_streams.pop(round_id, None)

    # 等待该轮的最终包或超时
    def _wait_for_round_streams(self, round_id: int, expected_clients: int, timeout_ms: int) -> Dict[int, "ClientStream"]:
        start = time.time() * 1000.0
        last_ready = -1

        while (time.time() * 1000.0 - start) < timeout_ms:
            with self._lock:
                m = self.round_streams.get(round_id, {})
                ready = sum(1 for cs in m.values() if cs.final_received)
            if ready != last_ready:
                print(f"[Controller_v3] progress: final-ready={ready}/{expected_clients}")
                last_ready = ready
            if ready >= expected_clients:
                break
            time.sleep(0.1)

        with self._lock:
            m = self.round_streams.get(round_id, {})
            if not m:
                return {}

            finals = {cid: cs for cid, cs in m.items() if cs.final_received}
            if finals:
                return finals

            print("[Controller_v3] WARNING: no final packets, aggregate with partial (may be inexact).", file=sys.stderr)
            return dict(m)

    # ========== 收集 & 语义 ==========
    class ClientVec:
        __slots__ = ("v", "num_samples", "is_delta")

        def __init__(self, v: List[float], num_samples: int, is_delta: bool):
            self.v = v
            self.num_samples = int(num_samples)
            self.is_delta = bool(is_delta)

    def _collect_client_vectors(self, streams: Dict[int, "ClientStream"]) -> List["ClientVec"]:
        out: List[ControllerV4.ClientVec] = []

        for cid, cs in streams.items():
            if not cs.packets:
                continue

            sum_vec: Optional[List[float]] = None
            any_delta = False
            any_weights = False

            for pkt in cs.packets:
                if self._is_s4_sparse(pkt):
                    v = self._decode_s4_to_float(pkt)
                    any_delta = True
                else:
                    v = self._bytes_to_float32_le(pkt)
                    any_weights = True

                if sum_vec is None:
                    sum_vec = list(v)
                else:
                    if len(sum_vec) != len(v):
                        raise RuntimeError("dim mismatch among packets")
                    for i in range(len(v)):
                        sum_vec[i] += v[i]

            is_delta = (any_delta and not any_weights)  # 与 Java 版一致
            n = cs.num_samples if cs.num_samples > 0 else 1
            out.append(ControllerV4.ClientVec(sum_vec, n, is_delta))

        return out

    # ========== 应用并发布 ==========
    def _apply_and_publish(self, cvs: List["ClientVec"], round_id: int):
        if not cvs:
            return

        any_weights = any(not cv.is_delta for cv in cvs)
        all_delta = all(cv.is_delta for cv in cvs)

        # 注意：与 Java 逻辑保持一致：
        # if any_weights && !all_delta -> 按“权重”FedAvg（即使混合，也走权重）
        # elif all_delta -> 平均Δ后叠加到 currentModel
        # else -> （理论上不会发生）混合错误
        if any_weights and not all_delta:
            dim = len(cvs[0].v)
            for cv in cvs:
                if len(cv.v) != dim:
                    raise RuntimeError("dim mismatch among clients")
            avg = [0.0] * dim
            tot = 0.0
            for cv in cvs:
                tot += max(1, cv.num_samples)
            for cv in cvs:
                coef = float(max(1, cv.num_samples)) / tot
                for i in range(dim):
                    avg[i] += cv.v[i] * coef
            self.current_model = avg
            self.model_dim = dim
            result = self.current_model
        elif all_delta:
            if self.current_model is None:
                print("[Controller_v3] WARNING: currentModel is null; bootstrap a weights round first!")
                dim = len(cvs[0].v)
                self.current_model = [0.0] * dim
                self.model_dim = dim
            for cv in cvs:
                if len(cv.v) != self.model_dim:
                    raise RuntimeError("dim mismatch to currentModel")
            delta = [0.0] * self.model_dim
            tot = 0.0
            for cv in cvs:
                tot += max(1, cv.num_samples)
            for cv in cvs:
                coef = float(max(1, cv.num_samples)) / tot
                for i in range(self.model_dim):
                    delta[i] += cv.v[i] * coef
            for i in range(self.model_dim):
                self.current_model[i] += delta[i]
            result = self.current_model
        else:
            raise RuntimeError("Mixed update types not supported in one round")

        model_bytes = self._float32_to_bytes_le(result)
        blob = dds.ModelBlob()
        blob.round_id = int(round_id)
        blob.data = model_bytes  # pybind 支持 bytes
        self.model_blob_writer.write(blob)
        print(f"[Controller_v3] published FP32 model, bytes={len(model_bytes)}")

    # ========== 评估（可选） ==========
    def _evaluate_model(self, model_data: bytes):
        print(f"[Controller_v3] Evaluating model, bytes={len(model_data)}")
        t0 = int(time.time() * 1000)
        try:
            tmp = Path.cwd() / f"eval_model_{int(time.time()*1000)}.bin"
            tmp.write_bytes(model_data)

            import subprocess
            cmd = [
                self.cfg.python_exe, self.cfg.eval_script,
                "--model", str(tmp),
                "--data_dir", str(self.cfg.data_dir),
                "--batch_size", str(self.cfg.batch_size),
            ]
            pb = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
            assert pb.stdout is not None
            for line in pb.stdout:
                print(f"[PY] {line.rstrip()}")
            exit_code = pb.wait()
            if exit_code != 0:
                print(f"[Controller_v3] Eval exit code: {exit_code}", file=sys.stderr)
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        except Exception:
            import traceback
            traceback.print_exc()
        print("[Controller_v3] Eval done.")

    # ========== 编解码/工具 ==========
    @staticmethod
    def _is_s4_sparse(b: Optional[bytes]) -> bool:
        if not b or len(b) < 4:
            return False
        return b[0] == ord('S') and b[1] == ord('4') and b[2] == 0 and b[3] == 1

    @staticmethod
    def _decode_s4_to_float(blob: bytes) -> List[float]:
        # 结构: 'S','4',0,1 | dim(int32 LE) | k(int32 LE) | idx[k](int32) | val[k](float32)
        import struct
        if not ControllerV4._is_s4_sparse(blob):
            raise ValueError("bad S4")
        off = 4
        dim, k = struct.unpack_from("<ii", blob, off); off += 8
        out = [0.0] * dim
        idx_fmt = f"<{k}i"
        val_fmt = f"<{k}f"
        idx = list(struct.unpack_from(idx_fmt, blob, off)); off += 4 * k
        vals = list(struct.unpack_from(val_fmt, blob, off))
        for i in range(k):
            id_ = idx[i]
            if 0 <= id_ < dim:
                out[id_] += vals[i]
        return out

    @staticmethod
    def _bytes_to_float32_le(data: bytes) -> List[float]:
        import struct
        if not data or (len(data) % 4) != 0:
            raise ValueError("fp32 bytes invalid")
        n = len(data) // 4
        return list(struct.unpack("<" + "f" * n, data))

    @staticmethod
    def _float32_to_bytes_le(v: List[float]) -> bytes:
        import struct
        return struct.pack("<" + "f" * len(v), *v)

    @staticmethod
    def _magic_of(b: Optional[bytes]) -> str:
        if not b or len(b) < 4:
            return "short"
        b0, b1, b2, b3 = b[0], b[1], b[2], b[3]
        if b0 == ord('S') and b1 == ord('4') and b2 == 0 and b3 == 1:
            return "S4/v1"
        if len(b) % 4 == 0:
            return "FP32(?)"
        return f"??({b0:02X} {b1:02X} {b2:02X} {b3:02X})"

    @staticmethod
    def _wait_reader_matched(reader, min_matches: int, timeout_ms: int) -> bool:
        start = time.time() * 1000.0
        st = dds.SubscriptionMatchedStatus()
        last = -1
        while (time.time() * 1000.0 - start) < timeout_ms:
            try:
                st = reader.get_subscription_matched_status()
            except Exception as e:
                print(f"[Controller_v3] reader status error: {e}", file=sys.stderr)
                return False
            if st.current_count != last:
                print(f"[Controller_v3] reader matched: current={st.current_count} total={st.total_count}")
                last = st.current_count
            if st.current_count >= min_matches:
                return True
            time.sleep(0.1)
        return False

    @staticmethod
    def _wait_writer_matched(writer, min_matches: int, timeout_ms: int) -> bool:
        start = time.time() * 1000.0
        st = dds.PublicationMatchedStatus()
        last = -1
        while (time.time() * 1000.0 - start) < timeout_ms:
            try:
                st = writer.get_publication_matched_status()
            except Exception as e:
                print(f"[Controller_v3] writer status error: {e}", file=sys.stderr)
                return False
            if st.current_count != last:
                print(f"[Controller_v3] writer matched: current={st.current_count} total={st.total_count}")
                last = st.current_count
            if st.current_count >= min_matches:
                return True
            time.sleep(0.1)
        return False

    # ========== 关停 ==========
    def shutdown(self):
        try:
            if self.client_update_reader is not None:
                self.client_update_reader.set_listener(None, 0)
            if self.dp is not None:
                self.dp.delete_contained_entities()
        except Exception as e:
            print(f"[Controller_v3] shutdown error: {e}")
        print("[Controller_v3] shutdown.")
        self._quit.set()


if __name__ == "__main__":
    ControllerV4.main(sys.argv[1:])
