# =================== Controller_merged.py ===================
# -*- coding: utf-8 -*-
"""
Controller: 接收三种格式并发布 FP32 模型
  - FP32 全量  (len % 4 == 0)
  - S4 稀疏Δ  header: 'S','4',0,1 | dim | k | idx[k] | val[k]
  - SQ8 稀疏量化Δ header: 'S','Q',0,1 | dim | k | chunk | nChunks | idx[k] | scales[nChunks] | q[k] (int8)
  - Q8 稠密权重 header: 'Q','8',0,1 | chunk | total | nChunks | scales[nChunks] | q[total]

聚合规则：
  - 若本轮全部是 Δ（S4 或 SQ8），则对 Δ 做加权平均后叠加到 current_model。
  - 若出现“权重型”（FP32/Q8），则按 FedAvg 更新权重（本版仍支持，但你说所有客户端都发 SQ8 就不会触发该分支）。
"""
import json, struct, subprocess, sys, threading, time
from pathlib import Path
from typing import Dict, List, Optional

import DDS_All as dds

# 可选：v3 barrier
try:
    from normal_distributed.dist_train_ddp_dgc.zrdds_allgather import ZrddsAllgather  # type: ignore
    from normal_distributed.dist_train_ddp_dgc.dds_barrier_verbose import ddp_barrier_verbose  # type: ignore
except Exception:
    ZrddsAllgather = None
    ddp_barrier_verbose = None

TOPIC_TRAIN_CMD     = "train_scripts/train_cmd"
TOPIC_CLIENT_UPDATE = "train_scripts/client_update"
TOPIC_MODEL_BLOB    = "train_scripts/model_blob"


class Controller:
    class Config:
        def __init__(self, j: dict):
            self.domain_id: int = int(j["domain_id"])
            self.expected_clients: int = int(j["expected_clients"])
            self.min_clients_to_aggregate: int = int(j.get("min_clients_to_aggregate", 1))
            self.timeout_ms: int = int(j["timeout_ms"])
            self.rounds: int = int(j.get("rounds", 1))
            self.python_exe: str = j.get("python_exe", "python")
            self.eval_script: str = j.get("eval_script", "").strip()
            self.data_dir: str = j.get("data_dir", "")
            self.batch_size: int = int(j.get("batch_size", 64))
            self.use_barrier: bool = bool(j.get("use_barrier", True))

    class ClientStream:
        __slots__ = ("packets", "num_samples", "final_received", "lock")
        def __init__(self):
            import threading as _th
            self.packets: List[bytes] = []
            self.num_samples: int = 0
            self.final_received: bool = False
            self.lock = _th.Lock()
        def add_packet(self, data: bytes, ns: int):
            with self.lock:
                self.packets.append(data)
                if ns and ns > 0:
                    self.final_received = True
                    self.num_samples = int(ns)

    class ClientVec:
        __slots__ = ("v", "num_samples", "is_delta")
        def __init__(self, v: List[float], num_samples: int, is_delta: bool):
            self.v = v
            self.num_samples = int(num_samples)
            self.is_delta = bool(is_delta)

    def __init__(self, cfg: "Controller.Config"):
        self.cfg = cfg
        self.dp = None
        self.publisher = None
        self.subscriber = None
        self.train_cmd_writer = None
        self.model_blob_writer = None
        self.client_update_reader = None
        self._client_update_listener = None
        self._ag = None

        self._round_counter = 1
        import threading as _th
        self._lock = _th.Lock()
        self.round_streams: Dict[int, Dict[int, Controller.ClientStream]] = {}

        self.current_model: Optional[List[float]] = None
        self.model_dim: int = -1

    # ---------- lifecycle ----------
    def init(self):
        dpf = dds.DomainParticipantFactory.get_instance()
        self.dp = dpf.create_participant(self.cfg.domain_id, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
        if not self.dp:
            raise RuntimeError("create participant failed")
        dds.register_all_types(self.dp)

        # barrier
        if self.cfg.use_barrier and ZrddsAllgather and ddp_barrier_verbose:
            self._ag = ZrddsAllgather(self.dp, topic="train_scripts/allgather_blob")
            self._wait_discovery(self._ag, world=self.cfg.expected_clients + 1, timeout_ms=max(10000, self.cfg.timeout_ms))
            ok = ddp_barrier_verbose(self._ag, group_id='1', rank=0, world=self.cfg.expected_clients + 1,
                                     domain_id=self.cfg.domain_id, topic_name="train_scripts/allgather_blob",
                                     min_writer_matches=self.cfg.expected_clients + 1,
                                     min_reader_matches=self.cfg.expected_clients + 1,
                                     match_timeout_s=150.0, barrier_timeout_s=600.0)
            if not ok:
                print("[Controller] barrier failed, continue without barrier...")
        else:
            print("[Controller] barrier disabled or unavailable, continue.")

        t_cmd = self.dp.create_topic(TOPIC_TRAIN_CMD, "TrainCmd", dds.TOPIC_QOS_DEFAULT, None, 0)
        t_upd = self.dp.create_topic(TOPIC_CLIENT_UPDATE, "ClientUpdate", dds.TOPIC_QOS_DEFAULT, None, 0)
        t_mdl = self.dp.create_topic(TOPIC_MODEL_BLOB, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)

        self.publisher = self.dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.subscriber = self.dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        wq = dds.DataWriterQos()
        self.publisher.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = 8

        self.train_cmd_writer = self.publisher.create_datawriter(t_cmd, wq, None, 0)
        self.model_blob_writer = self.publisher.create_datawriter(t_mdl, wq, None, 0)
        self._wait_writer_matched(self.train_cmd_writer, 1, 15000)
        self._wait_writer_matched(self.model_blob_writer, 1, 15000)

        rq = dds.DataReaderQos()
        self.subscriber.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        rq.history.depth = 64
        self.client_update_reader = self.subscriber.create_datareader(t_upd, rq, None, 0)
        if not self.client_update_reader:
            raise RuntimeError("create ClientUpdate reader failed")

        self._client_update_listener = ClientUpdateListener(self)
        self.client_update_reader.set_listener(self._client_update_listener, dds.StatusKind.DATA_AVAILABLE_STATUS)
        self._wait_reader_matched(self.client_update_reader, 1, 15000)

        print("[Controller] ready.")

    def shutdown(self):
        try:
            if self.client_update_reader is not None:
                self.client_update_reader.set_listener(None, 0)
            if self._ag is not None:
                try:
                    if getattr(self._ag, "reader", None) is not None:
                        self._ag.reader.set_listener(None, 0)
                    if getattr(self._ag, "writer", None) is not None:
                        self._ag.writer.set_listener(None, 0)
                except Exception:
                    pass
            if self.dp is not None:
                self.dp.delete_contained_entities()
        except Exception as e:
            print(f"[Controller] shutdown error: {e}")
        print("[Controller] shutdown.")

    # ---------- round ----------
    def run_round(self, subset_size: int, epochs: int, lr: float, seed: int):
        round_id = self._round_counter
        self._round_counter += 1

        print(f"\n================ Round {round_id} ================")
        t0 = int(time.time() * 1000)

        cmd = dds.TrainCmd()
        cmd.round_id   = int(round_id)
        cmd.subset_size= int(subset_size)
        cmd.epochs     = int(epochs)
        cmd.lr         = float(lr)
        cmd.seed       = int(seed)
        self.train_cmd_writer.write(cmd)
        print(f"[Controller] TrainCmd written: round={round_id}")

        streams = self._wait_for_streams(round_id, self.cfg.expected_clients, self.cfg.min_clients_to_aggregate, self.cfg.timeout_ms)
        if not streams:
            print("[Controller] no updates collected; skip this round.")
            return

        print(f"[Controller] collected clients: {len(streams)} / expected {self.cfg.expected_clients}")
        cvs = self._collect_vectors(streams)
        self._apply_and_publish(cvs, round_id)

        t1 = int(time.time() * 1000)
        print(f"[Controller] round time: {t1 - t0} ms")

        if self.current_model is not None and self.cfg.eval_script:
            self._evaluate_model(self._float32_to_bytes(self.current_model))

        with self._lock:
            self.round_streams.pop(round_id, None)

    # ---------- data path ----------
    def on_update(self, cu):
        payload: bytes = cu.data
        rnd = int(cu.round_id)
        cid = int(cu.client_id)
        ns  = int(cu.num_samples)

        with self._lock:
            m = self.round_streams.setdefault(rnd, {})
            cs = m.setdefault(cid, Controller.ClientStream())
            cs.add_packet(payload, ns)

        print(f"[Controller] recv: round={rnd} client={cid} bytes={0 if payload is None else len(payload)} ns={ns} magic={self._magic_of(payload)}")

    # ---------- wait & collect ----------
    def _wait_for_streams(self, round_id: int, expected_clients: int, min_clients: int, timeout_ms: int) -> Dict[int, "Controller.ClientStream"]:
        start = time.time() * 1000.0
        last_ready = -1
        while (time.time() * 1000.0 - start) < timeout_ms:
            with self._lock:
                m = self.round_streams.get(round_id, {})
                ready = sum(1 for cs in m.values() if cs.final_received)
            if ready != last_ready:
                print(f"[Controller] progress: final-ready={ready}/{expected_clients} (min={min_clients})")
                last_ready = ready
            if ready >= min_clients:
                break
            time.sleep(0.1)
        with self._lock:
            m = self.round_streams.get(round_id, {})
            if not m:
                return {}
            finals = {cid: cs for cid, cs in m.items() if cs.final_received}
            return finals if finals else dict(m)

    def _collect_vectors(self, streams: Dict[int, "Controller.ClientStream"]) -> List["Controller.ClientVec"]:
        out: List[Controller.ClientVec] = []
        for cid, cs in streams.items():
            if not cs.packets:
                continue
            sum_vec: Optional[List[float]] = None
            any_delta = False
            any_weights = False
            for pkt in cs.packets:
                if self._is_sq8(pkt):
                    v = self._decode_sq8(pkt)   # 稀疏量化Δ
                    any_delta = True
                elif self._is_s4(pkt):
                    v = self._decode_s4(pkt)    # 稀疏浮点Δ
                    any_delta = True
                elif self._is_q8(pkt):
                    v = self._decode_q8(pkt)    # 稠密权重
                    any_weights = True
                else:
                    v = self._bytes_to_float32(pkt)  # FP32 权重
                    any_weights = True
                if sum_vec is None:
                    sum_vec = list(v)
                else:
                    if len(sum_vec) != len(v):
                        raise RuntimeError("dim mismatch among packets")
                    for i in range(len(v)):
                        sum_vec[i] += v[i]
            if sum_vec is None:
                continue
            is_delta = (any_delta and not any_weights)
            n = cs.num_samples if cs.num_samples > 0 else 1
            out.append(Controller.ClientVec(sum_vec, n, is_delta))
        return out

    def _apply_and_publish(self, cvs: List["Controller.ClientVec"], round_id: int):
        if not cvs:
            return
        any_weights = any(not cv.is_delta for cv in cvs)
        all_delta   = all(cv.is_delta for cv in cvs)

        if any_weights and not all_delta:
            # FedAvg 权重
            dim = len(cvs[0].v)
            for cv in cvs:
                if len(cv.v) != dim:
                    raise RuntimeError("dim mismatch among clients")
            avg = [0.0] * dim
            tot = float(sum(max(1, cv.num_samples) for cv in cvs))
            for cv in cvs:
                coef = float(max(1, cv.num_samples)) / tot
                for i in range(dim):
                    avg[i] += cv.v[i] * coef
            self.current_model = avg
            self.model_dim = dim
            result = self.current_model
        elif all_delta:
            # 平均 Δ 后叠加
            if self.current_model is None:
                print("[Controller] WARNING: current model is None; bootstrap weights first!")
                dim = len(cvs[0].v)
                self.current_model = [0.0] * dim
                self.model_dim = dim
            for cv in cvs:
                if len(cv.v) != self.model_dim:
                    raise RuntimeError("dim mismatch to current model")
            delta = [0.0] * self.model_dim
            tot = float(sum(max(1, cv.num_samples) for cv in cvs))
            for cv in cvs:
                coef = float(max(1, cv.num_samples)) / tot
                for i in range(self.model_dim):
                    delta[i] += cv.v[i] * coef
            for i in range(self.model_dim):
                self.current_model[i] += delta[i]
            result = self.current_model
        else:
            raise RuntimeError("unreachable")

        blob = dds.ModelBlob()
        blob.round_id = int(round_id)
        blob.data = self._float32_to_bytes(result)
        self.model_blob_writer.write(blob)
        print(f"[Controller] published FP32 model, bytes={len(blob.data)}")

    # ---------- eval ----------
    def _evaluate_model(self, model_bytes: bytes):
        try:
            tmp = Path.cwd() / f"eval_model_{int(time.time()*1000)}.bin"
            tmp.write_bytes(model_bytes)
            if not self.cfg.eval_script:
                return
            cmd = [self.cfg.python_exe, self.cfg.eval_script, "--model", str(tmp),
                   "--data_dir", str(self.cfg.data_dir), "--batch_size", str(self.cfg.batch_size)]
            print(f"[Controller] spawn eval: {' '.join(cmd)}")
            pb = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
            assert pb.stdout is not None
            for line in pb.stdout:
                print(f"[PY] {line.rstrip()}")
            code = pb.wait()
            print(f"[Controller] Eval exit={code}")
            try: tmp.unlink(missing_ok=True)
            except Exception: pass
        except Exception:
            import traceback; traceback.print_exc()

    # ---------- decoders ----------
    @staticmethod
    def _is_s4(b: Optional[bytes]) -> bool:
        return bool(b) and len(b) >= 4 and b[0]==ord('S') and b[1]==ord('4') and b[2]==0 and b[3]==1

    @staticmethod
    def _decode_s4(blob: bytes) -> List[float]:
        if not Controller._is_s4(blob):
            raise ValueError("bad S4 header")
        off = 4
        dim, k = struct.unpack_from("<ii", blob, off); off += 8
        out = [0.0]*dim
        if k <= 0: return out
        idx = list(struct.unpack_from(f"<{k}i", blob, off)); off += 4*k
        vals = list(struct.unpack_from(f"<{k}f", blob, off))
        for i in range(k):
            ii = idx[i]
            if 0 <= ii < dim:
                out[ii] += vals[i]
        return out

    @staticmethod
    def _is_sq8(b: Optional[bytes]) -> bool:
        return bool(b) and len(b) >= 4 and b[0]==ord('S') and b[1]==ord('Q') and b[2]==0 and b[3]==1

    @staticmethod
    def _decode_sq8(blob: bytes) -> List[float]:
        """'S','Q',0,1 | dim(i32) | k(i32) | chunk(i32) | nChunks(i32) | idx[k](i32) | scales[n](f32) | q[k](i8)"""
        bb = memoryview(blob)
        if not Controller._is_sq8(bb):
            raise ValueError("bad SQ8 header")
        off = 4
        dim, k, chunk, nChunks = struct.unpack_from("<iiii", bb, off); off += 16
        out = [0.0]*dim
        if k <= 0: return out
        idx = list(struct.unpack_from(f"<{k}i", bb, off)); off += 4*k
        scales = [struct.unpack_from("<f", bb, off + 4*i)[0] for i in range(nChunks)]
        off += 4*nChunks
        for i in range(k):
            q = struct.unpack_from("b", bb, off+i)[0]
            ci = i // max(1, chunk)
            sc = scales[ci] if 0 <= ci < nChunks else 1.0
            ii = idx[i]
            if 0 <= ii < dim:
                out[ii] += float(q) * float(sc)
        return out

    @staticmethod
    def _is_q8(b: Optional[bytes]) -> bool:
        return bool(b) and len(b) >= 4 and b[0]==ord('Q') and b[1]==ord('8') and b[2]==0 and b[3]==1

    @staticmethod
    def _decode_q8(blob: bytes) -> List[float]:
        bb = memoryview(blob)
        if not Controller._is_q8(bb): raise ValueError("bad Q8 header")
        chunk = int.from_bytes(bb[4:8],'little')
        total = int.from_bytes(bb[8:16],'little', signed=False)
        nChunks = int.from_bytes(bb[16:20],'little')
        scales = [struct.unpack_from('<f', bb, 20 + 4*i)[0] for i in range(nChunks)]
        out = [0.0]*total
        off = 20 + 4*nChunks
        for ci in range(nChunks):
            s, e, sc = ci*chunk, min((ci+1)*chunk, total), scales[ci]
            for j in range(s, e):
                q = struct.unpack_from('b', bb, off)[0]; off += 1
                out[j] = float(q) * sc
        return out

    @staticmethod
    def _bytes_to_float32(data: bytes) -> List[float]:
        if not data or (len(data) % 4) != 0: raise ValueError("fp32 bytes invalid")
        n = len(data)//4
        return list(struct.unpack("<" + "f"*n, data))

    @staticmethod
    def _float32_to_bytes(v: List[float]) -> bytes:
        return struct.pack("<" + "f"*len(v), *[float(x) for x in v])

    @staticmethod
    def _magic_of(b: Optional[bytes]) -> str:
        if not b or len(b)<4: return "short"
        if Controller._is_sq8(b): return "SQ8/v1"
        if Controller._is_s4(b):  return "S4/v1"
        if Controller._is_q8(b):  return "Q8/v1"
        if len(b)%4==0:           return "FP32(?)"
        return f"??({b[0]:02X} {b[1]:02X} {b[2]:02X} {b[3]:02X})"

    # ---------- DDS helpers ----------
    @staticmethod
    def _wait_writer_matched(writer, min_matches: int, timeout_ms: int) -> bool:
        start = time.time()*1000.0; last = -1
        st = dds.PublicationMatchedStatus()
        while (time.time()*1000.0 - start) < timeout_ms:
            try: st = writer.get_publication_matched_status()
            except Exception as e:
                print(f"[Controller] writer status error: {e}"); return False
            if st.current_count != last:
                print(f"[Controller] writer matched: current={st.current_count} total={st.total_count}")
                last = st.current_count
            if st.current_count >= min_matches: return True
            time.sleep(0.1)
        return False

    @staticmethod
    def _wait_reader_matched(reader, min_matches: int, timeout_ms: int) -> bool:
        start = time.time()*1000.0; last = -1
        st = dds.SubscriptionMatchedStatus()
        while (time.time()*1000.0 - start) < timeout_ms:
            try: st = reader.get_subscription_matched_status()
            except Exception as e:
                print(f"[Controller] reader status error: {e}"); return False
            if st.current_count != last:
                print(f"[Controller] reader matched: current={st.current_count} total={st.total_count}")
                last = st.current_count
            if st.current_count >= min_matches: return True
            time.sleep(0.1)
        return False

    @staticmethod
    def _wait_discovery(ag, world: int, timeout_ms: int = 10000, include_self: bool = True, poll_ms: int = 200):
        if ag is None: return
        deadline = time.time() + timeout_ms/1000.0
        target = world if include_self else max(0, world-1)
        def _pub():  return int(getattr(ag.writer.get_publication_matched_status(), "current_count", 0))
        def _sub():  return int(getattr(ag.reader.get_subscription_matched_status(), "current_count", 0))
        last_w = last_r = -1
        while True:
            cw, cr = _pub(), _sub()
            if cw >= target and cr >= target:
                print(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}"); return
            if cw != last_w or cr != last_r:
                print(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}")
                last_w, last_r = cw, cr
            if time.time() >= deadline:
                print(f"[ag][discovery] timeout: writer={cw}, reader={cr}, target={target}"); return
            time.sleep(poll_ms/1000.0)


class ClientUpdateListener(dds.DataReaderListener):
    def __init__(self, outer: Controller):
        super().__init__(); self.outer = outer

    @staticmethod
    def _latency_ms_from_info(info):
        """返回 DDS 样本的近似传输时间（ms）。
        利用 write() 端的 source_timestamp 与接收端当前时间差."""
        try:
            st = getattr(info, "source_timestamp", None)
            if st is None:
                return None
            # 常见绑定：st.sec / st.nanosec；也兼容 (sec, nsec) 或 float 秒
            if hasattr(st, "sec") and hasattr(st, "nanosec"):
                send_ms = int(st.sec) * 1000 + int(st.nanosec) // 1_000_000
            elif isinstance(st, (tuple, list)) and len(st) >= 2:
                send_ms = int(st[0]) * 1000 + int(st[1]) // 1_000_000
            else:
                send_ms = int(float(st) * 1000.0)
            now_ms = int(time.time() * 1000)
            return max(0, now_ms - send_ms)
        except Exception:
            return None

    def on_data_available(self, reader):
        try:
            samples = dds.ClientUpdateSeq(); infos = dds.SampleInfoSeq()
            reader.take(samples, infos, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
            try:
                for i in range(samples.length()):
                    if not infos.get_at(i).valid_data: continue
                    lat_ms = self._latency_ms_from_info(infos.get_at(i))
                    if lat_ms is not None:
                        s = samples.get_at(i)
                        sz = 0 if (not hasattr(s, "data") or s.data is None) else len(s.data)
                        rid = int(getattr(s, "round_id", -1))
                        cid = int(getattr(s, "client_id", -1))
                        print(f"[DDS][uplink] transit≈{lat_ms} ms (round={rid} client={cid} bytes={sz})")
                    self.outer.on_update(samples.get_at(i))
            finally:
                reader.return_loan(samples, infos)
        except Exception:
            import traceback; traceback.print_exc()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python Controller_merged.py <controller_merged.conf.json>"); sys.exit(1)
    cfg = Controller.Config(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")))
    ctrl = Controller(cfg)
    ctrl.init()
    try:
        for i in range(cfg.rounds):
            ctrl.run_round(subset_size=6000, epochs=5, lr=0.01, seed=12345+i)
    finally:
        ctrl.shutdown()
