# =================== Client_merged.py ===================
# -*- coding: utf-8 -*-
"""
Client wire_format:
  - "fp32"   : 直接全量权重
  - "fp32_sparse" : S4 浮点稀疏Δ（可流式）
  - "int8_dense"  : Q8 稠密权重（单包）
  - "sq8"         : 稀疏 + INT8 量化 Δ（本版新增；基于 S4 产物离线量化）

* 当 wire_format="sq8" 时，训练仍走 v3 稀疏产物（--compres fp32_sparse），客户端把 .s4 包转换为 .sq8 再发送。
* SQ8 打包格式：'S','Q',0,1 | dim | k | chunk | nChunks | idx[k] | scales[n] | q[k]
  - 将 k 个非零分成 nChunks=ceil(k/chunk)，每块一个 scale = max(|vals_chunk|)/127（=0 时置 1.0）。
"""
import json as _json, os as _os, signal as _signal, subprocess as _subprocess, sys as _sys, tempfile as _tempfile
import time as _time, struct, math
from pathlib import Path as _Path
from typing import List as _List, Optional as _Optional

import DDS_All as dds

try:
    from normal_distributed.dist_train_ddp_dgc.zrdds_allgather import ZrddsAllgather  # type: ignore
    from normal_distributed.dist_train_ddp_dgc.dds_barrier_verbose import ddp_barrier_verbose  # type: ignore
except Exception:
    ZrddsAllgather = None
    ddp_barrier_verbose = None

TOPIC_TRAIN_CMD     = "train_scripts/train_cmd"
TOPIC_CLIENT_UPDATE = "train_scripts/client_update"
TOPIC_MODEL_BLOB    = "train_scripts/model_blob"


class Client:
    DOMAIN_ID = 0
    CLIENT_ID = 0
    NUM_CLIENTS = 1
    BATCH_SIZE = 32
    DATA_DIR = ""
    PYTHON_EXE = "python"
    TRAINER_PY = ""

    WIRE_FORMAT = "fp32"     # fp32_full | fp32_sparse | int8_dense | sq8
    STREAM = True
    COMM_EVERY = 0
    SPARSE_FIRST_ROUND = False
    SPARSE_RATIO = 0.1

    INT8_CHUNK = 1024      # for int8_dense (dense Q8)
    SQ8_CHUNK  = 1024      # for sq8: 每块非零个数，用于逐块 scale

    INIT_MODEL_PATH = ""
    WAIT_MODEL_MS = 8000

    def __init__(self):
        self.dp = None; self.pub = None; self.sub = None
        self.t_cmd = None; self.t_upd = None; self.t_model = None
        self.cmd_reader = None; self.model_reader = None; self.upd_writer = None
        self.cmd_listener = None; self.model_listener = None
        self.last_round = -1
        self.latest_model_dir = _Path(_os.getcwd()) / "global_model"
        self.latest_model_path = self.latest_model_dir / "latest_model.bin"
        self.latest_model_round = -1
        self._quit = False

    # ---------- CLI ----------
    @staticmethod
    def main(argv: _List[str]):
        if len(argv) != 1:
            print("Usage: python Client_merged.py <client_merged.conf.json>", file=_sys.stderr); return
        try:
            Client.load_config(argv[0])
            node = Client()
            def _on_sig(sig, frame):
                try: node.shutdown()
                finally: pass
            _signal.signal(_signal.SIGINT, _on_sig); _signal.signal(_signal.SIGTERM, _on_sig)
            node.start()
            while not node._quit: _time.sleep(0.1)
        except Exception:
            import traceback as _tb; _tb.print_exc()

    @staticmethod
    def load_config(conf_path: str):
        j = _json.loads(_Path(conf_path).read_text(encoding="utf-8"))
        Client.DOMAIN_ID = int(j["domain_id"])
        Client.CLIENT_ID = int(j["client_id"])
        Client.NUM_CLIENTS = int(j["num_clients"])
        Client.BATCH_SIZE = int(j.get("batch_size", 32))
        Client.DATA_DIR = j.get("data_dir", "")
        Client.PYTHON_EXE = j.get("python_exe", _os.environ.get("PYTHON_EXE", "python"))
        Client.TRAINER_PY = j.get("trainer_script", "").strip()

        Client.WIRE_FORMAT = j.get("wire_format", "fp32").strip().lower()
        Client.STREAM = bool(j.get("stream", True))
        Client.COMM_EVERY = int(j.get("comm_every", 0))
        Client.SPARSE_FIRST_ROUND = bool(j.get("sparse_first_round", False))
        Client.SPARSE_RATIO = float(j.get("sparse_ratio", 0.1))

        Client.INT8_CHUNK = int(j.get("int8_chunk", 1024))
        Client.SQ8_CHUNK  = int(j.get("sq8_chunk", 1024))

        Client.INIT_MODEL_PATH = j.get("init_model_path", "").strip()
        Client.WAIT_MODEL_MS = int(j.get("wait_model_ms", 8000))

        print(f"[Client] cfg: domain={Client.DOMAIN_ID} client={Client.CLIENT_ID} num_clients={Client.NUM_CLIENTS} "
              f"wire_format={Client.WIRE_FORMAT} stream={Client.STREAM} comm_every={Client.COMM_EVERY} "
              f"sparse_ratio={Client.SPARSE_RATIO} sparse_first_round={Client.SPARSE_FIRST_ROUND} "
              f"sq8_chunk={Client.SQ8_CHUNK} "
              + (f"init_model_path={Client.INIT_MODEL_PATH}" if Client.INIT_MODEL_PATH else ""))

    # ---------- start/shutdown ----------
    def start(self):
        try: self.latest_model_dir.mkdir(parents=True, exist_ok=True)
        except Exception: pass

        self.dp = dds.DomainParticipantFactory.get_instance().create_participant(
            Client.DOMAIN_ID, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
        dds.register_all_types(self.dp)

        if ZrddsAllgather and ddp_barrier_verbose:
            try:
                ag = ZrddsAllgather(self.dp, topic="train_scripts/allgather_blob")
                self._wait_discovery(ag, world=Client.NUM_CLIENTS + 1, timeout_ms=max(10000, Client.WAIT_MODEL_MS))
                ok = ddp_barrier_verbose(ag, group_id='1', rank=Client.CLIENT_ID + 1,
                                         world=Client.NUM_CLIENTS + 1, domain_id=Client.DOMAIN_ID,
                                         topic_name="train_scripts/allgather_blob",
                                         min_writer_matches=Client.NUM_CLIENTS + 1,
                                         min_reader_matches=Client.NUM_CLIENTS + 1,
                                         match_timeout_s=150.0, barrier_timeout_s=600.0)
                if not ok: print("[Client] barrier failed; continue anyway.")
            except Exception:
                print("[Client] barrier unavailable; continue.")

        self.t_cmd = self.dp.create_topic(TOPIC_TRAIN_CMD, "TrainCmd", dds.TOPIC_QOS_DEFAULT, None, 0)
        self.t_upd = self.dp.create_topic(TOPIC_CLIENT_UPDATE, "ClientUpdate", dds.TOPIC_QOS_DEFAULT, None, 0)
        self.t_model = self.dp.create_topic(TOPIC_MODEL_BLOB, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)

        self.pub = self.dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = self.dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        wq = dds.DataWriterQos(); self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS; wq.history.depth = 8
        self.upd_writer = self.pub.create_datawriter(self.t_upd, wq, None, 0)
        self._wait_writer_matched(self.upd_writer, 1, 15000)

        rq = dds.DataReaderQos(); self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS; rq.history.depth = 8
        self.cmd_reader = self.sub.create_datareader(self.t_cmd, rq, None, 0)
        self._wait_reader_matched(self.cmd_reader, 1, 15000)
        self.model_reader = self.sub.create_datareader(self.t_model, rq, None, 0)
        self._wait_reader_matched(self.model_reader, 1, 15000)

        self.cmd_listener = _TrainCmdListener(self)
        self.model_listener = _ModelBlobListener(self)
        self.cmd_reader.set_listener(self.cmd_listener, dds.StatusKind.DATA_AVAILABLE_STATUS)
        self.model_reader.set_listener(self.model_listener, dds.StatusKind.DATA_AVAILABLE_STATUS)

        print("[Client] started. Waiting for TrainCmd...")

    def shutdown(self):
        try:
            if self.cmd_reader is not None: self.cmd_reader.set_listener(None, 0)
            if self.model_reader is not None: self.model_reader.set_listener(None, 0)
            if self.dp is not None: self.dp.delete_contained_entities()
        except Exception as e:
            print(f"[Client] shutdown error: {e}")
        print("[Client] shutdown."); self._quit = True

    # ---------- run training ----------
    def run_training(self, round_id: int, subset: int, epochs: int, lr: float, seed: int):
        """Return:
           - stream: {"mode":"stream","num_samples":N,"packets":[bytes,...]}
           - single: {"mode":"single","num_samples":N,"bytes":bytes}
        """
        first_round_sparse = (Client.WIRE_FORMAT in ("fp32_sparse","sq8") and Client.SPARSE_FIRST_ROUND and round_id==1)
        if round_id > 1 and not first_round_sparse:
            self._wait_prev_model(round_id)

        compress = Client.WIRE_FORMAT
        # SQ8 实际训练仍用 S4
        underlying = "fp32_sparse" if compress=="sq8" else compress

        init_path: _Optional[_Path] = None
        if Client.INIT_MODEL_PATH and _Path(Client.INIT_MODEL_PATH).exists():
            init_path = _Path(Client.INIT_MODEL_PATH)
        elif self.latest_model_path.exists():
            init_path = self.latest_model_path

        stream_mode = (underlying == "fp32_sparse") and Client.STREAM and (Client.COMM_EVERY >= 0)

        cmd: _List[str] = [Client.PYTHON_EXE, Client.TRAINER_PY]
        out_dir = None; out_bin = None
        if stream_mode:
            out_dir = _Path(_tempfile.mkdtemp(prefix="upd_stream_")); cmd += ["--out", str(out_dir)]
        else:
            out_bin = _Path(_tempfile.mkstemp(prefix="upd_", suffix=".bin")[1]); cmd += ["--out", str(out_bin)]

        cmd += [
            "--client_id", str(Client.CLIENT_ID), "--num_clients", str(Client.NUM_CLIENTS),
            "--seed", str(seed), "--epochs", str(epochs), "--lr", str(lr),
            "--batch_size", str(Client.BATCH_SIZE), "--data_dir", str(Client.DATA_DIR),
            "--round", str(round_id), "--subset", str(subset),
            "--compress", underlying, "--state_dir", str(_Path(Client.DATA_DIR) / f"client_{Client.CLIENT_ID}_state"),
        ]
        if underlying == "fp32_sparse":
            cmd += ["--sparse_ratio", str(Client.SPARSE_RATIO), "--comm_every", str(Client.COMM_EVERY)]
        if underlying == "int8_dense":
            cmd += ["--chunk", str(Client.INT8_CHUNK)]
        if init_path is not None:
            cmd += ["--init_model", str(init_path.resolve())]; print(f"[Client] init from model: {init_path.resolve()}")
        else:
            print("[Client] no init model found, cold start this round.")

        print(f"[Client] spawn trainer: {' '.join(map(str, cmd))}")
        p = _subprocess.Popen(cmd, stdout=_subprocess.PIPE, stderr=_subprocess.STDOUT, text=True, encoding="utf-8")
        stdout_lines: _List[str] = []; assert p.stdout is not None
        for line in p.stdout:
            line = line.rstrip("\n"); print(f"[PY] {line}"); stdout_lines.append(line)
        code = p.wait()
        if code != 0: raise RuntimeError(f"trainer exit={code}")
        num_samples = self._parse_num_samples("\n".join(stdout_lines))

        if stream_mode:
            packets_s4 = self._collect_s4_packets(_Path(out_dir))
            self._safe_delete_dir(_Path(out_dir))
            if compress == "sq8":
                packets_sq8 = [self._s4_to_sq8_bytes(pkt, Client.SQ8_CHUNK) for pkt in packets_s4]
                return {"mode": "stream", "num_samples": num_samples, "packets": packets_sq8}
            else:
                return {"mode": "stream", "num_samples": num_samples, "packets": packets_s4}
        else:
            with open(out_bin, "rb") as f: data = f.read()
            try: _Path(out_bin).unlink(missing_ok=True)
            except Exception: pass
            if compress == "sq8":
                data = self._s4_to_sq8_bytes(data, Client.SQ8_CHUNK)
            return {"mode": "single", "num_samples": num_samples, "bytes": data}

    # ---------- S4->SQ8 转换 ----------
    @staticmethod
    def _s4_to_sq8_bytes(s4: bytes, chunk: int) -> bytes:
        """S4: 'S','4',0,1 | dim | k | idx[k] | vals[k](f32)
           SQ8: 'S','Q',0,1 | dim | k | chunk | nChunks | idx[k] | scales[n] | q[k]"""
        if not (s4 and len(s4) >= 12 and s4[0]==ord('S') and s4[1]==ord('4') and s4[2]==0 and s4[3]==1):
            raise ValueError("not a valid S4 blob")
        off = 4
        dim, k = struct.unpack_from("<ii", s4, off); off += 8
        idx = list(struct.unpack_from(f"<{k}i", s4, off)); off += 4*k
        vals = list(struct.unpack_from(f"<{k}f", s4, off))
        if k <= 0:
            return struct.pack("<cccciiii", b'S', b'Q', b'\x00', b'\x01', dim, 0, max(1, chunk), 0)

        C = max(1, int(chunk))
        nChunks = (k + C - 1) // C

        # 计算每块的 scale，并量化
        scales = [0.0]*nChunks
        qbytes = bytearray(k)
        for ci in range(nChunks):
            s = ci*C
            e = min(s + C, k)
            # 绝对值最大值
            amax = 0.0
            for i in range(s, e):
                v = abs(vals[i]);
                if v > amax: amax = v
            sc = (amax / 127.0) if amax > 0.0 else 1.0
            scales[ci] = float(sc)
            inv = 1.0 / sc
            for i in range(s, e):
                q = int(round(vals[i] * inv))
                if q < -127: q = -127
                if q >  127: q =  127
                qbytes[i] = (q + 256) % 256  # pack as signed int8 later

        # 打包 SQ8
        header = struct.pack("<cccciiii", b'S', b'Q', b'\x00', b'\x01', int(dim), int(k), int(C), int(nChunks))
        idx_bytes = struct.pack("<" + "i"*k, *idx)
        scales_bytes = struct.pack("<" + "f"*nChunks, *scales)
        q_as_int8 = struct.pack("<" + "b"*k, *([int(q)-256 if q>127 else int(q) for q in qbytes]))
        return header + idx_bytes + scales_bytes + q_as_int8

    # ---------- helpers ----------
    def _wait_prev_model(self, round_id: int):
        need_round = round_id - 1
        print(f"[Client] waiting global model for prev round = {need_round}")
        start = _time.time()*1000.0; maxw = max(Client.WAIT_MODEL_MS, 0)
        while True:
            have = self.latest_model_round
            if have >= need_round and self.latest_model_path.exists():
                print(f"[Client] found latest_model.bin for round {need_round}"); return
            if (_time.time()*1000.0 - start) > maxw:
                print(f"[Client] WARN: waited {int(_time.time()*1000.0 - start)} ms but no model for round {need_round}; start cold."); return
            _time.sleep(0.1)

    @staticmethod
    def _parse_num_samples(text: str) -> int:
        try:
            l = text.rfind("{"); r = text.rfind("}")
            if l>=0 and r>l:
                obj = _json.loads(text[l:r+1])
                if "num_samples" in obj: return int(obj["num_samples"])
        except Exception:
            import traceback as _tb; _tb.print_exc()
        return 0

    @staticmethod
    def _collect_s4_packets(dir_path: _Path) -> _List[bytes]:
        if not dir_path.exists(): return []
        files = sorted([p for p in dir_path.glob("*.s4")], key=lambda p: p.name)
        out: _List[bytes] = []
        for p in files:
            with open(p, "rb") as f: out.append(f.read())
        return out

    @staticmethod
    def _safe_delete_dir(dir_path: _Path):
        try:
            if not dir_path.exists(): return
            for p in sorted(dir_path.rglob("*"), key=lambda x: len(str(x)), reverse=True):
                try:
                    if p.is_file() or p.is_symlink(): p.unlink(missing_ok=True)
                    else: p.rmdir()
                except Exception: pass
            try: dir_path.rmdir()
            except Exception: pass
        except Exception: pass

    @staticmethod
    def _wait_reader_matched(reader, min_matches: int, timeout_ms: int) -> bool:
        start=_time.time()*1000.0; last=-1
        st = dds.SubscriptionMatchedStatus()
        while (_time.time()*1000.0 - start) < timeout_ms:
            try: st = reader.get_subscription_matched_status()
            except Exception as e: print(f"[Client] reader status error: {e}"); return False
            if st.current_count != last:
                print(f"[Client] reader matched: current={st.current_count} total={st.total_count}"); last = st.current_count
            if st.current_count >= min_matches: return True
            _time.sleep(0.1)
        return False

    @staticmethod
    def _wait_writer_matched(writer, min_matches: int, timeout_ms: int) -> bool:
        start=_time.time()*1000.0; last=-1
        st = dds.PublicationMatchedStatus()
        while (_time.time()*1000.0 - start) < timeout_ms:
            try: st = writer.get_publication_matched_status()
            except Exception as e: print(f"[Client] writer status error: {e}"); return False
            if st.current_count != last:
                print(f"[Client] updWriter matched: current={st.current_count} total={st.total_count}"); last = st.current_count
            if st.current_count >= min_matches: return True
            _time.sleep(0.1)
        return False

    @staticmethod
    def _wait_discovery(ag, world: int, timeout_ms: int = 10000, include_self: bool = True, poll_ms: int = 200):
        if ag is None: return
        deadline = _time.time() + timeout_ms/1000.0
        target = world if include_self else max(0, world-1)
        def _pub():  return int(getattr(ag.writer.get_publication_matched_status(), "current_count", 0))
        def _sub():  return int(getattr(ag.reader.get_subscription_matched_status(), "current_count", 0))
        last_w = last_r = -1
        while True:
            cw, cr = _pub(), _sub()
            if cw>=target and cr>=target: print(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}"); return
            if cw!=last_w or cr!=last_r:
                print(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}"); last_w, last_r = cw, cr
            if _time.time() >= deadline:
                print(f"[ag][discovery] timeout: writer={cw}, reader={cr}, target={target}"); return
            _time.sleep(poll_ms/1000.0)


class _TrainCmdListener(dds.DataReaderListener):
    def __init__(self, cli: Client):
        super().__init__(); self.client = cli

    @staticmethod
    def _latency_ms_from_info(info):
        try:
            st = getattr(info, "source_timestamp", None)
            if st is None:
                return None
            if hasattr(st, "sec") and hasattr(st, "nanosec"):
                send_ms = int(st.sec) * 1000 + int(st.nanosec) // 1_000_000
            elif isinstance(st, (tuple, list)) and len(st) >= 2:
                send_ms = int(st[0]) * 1000 + int(st[1]) // 1_000_000
            else:
                send_ms = int(float(st) * 1000.0)
            now_ms = int(_time.time() * 1000)
            return max(0, now_ms - send_ms)
        except Exception:
            return None

    @staticmethod
    def _latency_ms_from_info(info):
        try:
            st = getattr(info, "source_timestamp", None)
            if st is None:
                return None
            if hasattr(st, "sec") and hasattr(st, "nanosec"):
                send_ms = int(st.sec) * 1000 + int(st.nanosec) // 1_000_000
            elif isinstance(st, (tuple, list)) and len(st) >= 2:
                send_ms = int(st[0]) * 1000 + int(st[1]) // 1_000_000
            else:
                send_ms = int(float(st) * 1000.0)
            now_ms = int(_time.time() * 1000)
            return max(0, now_ms - send_ms)
        except Exception:
            return None

    def on_data_available(self, reader):
        try:
            samples = dds.TrainCmdSeq();
            infos = dds.SampleInfoSeq()
            reader.take(samples, infos, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
            try:
                for i in range(samples.length()):
                    if not infos.get_at(i).valid_data:
                        continue
                    # >>> 新增：打印 controller→client 的 TrainCmd 传输时间
                    lat_ms = self._latency_ms_from_info(infos.get_at(i))
                    if lat_ms is not None:
                        rid = int(getattr(samples.get_at(i), "round_id", -1))
                        print(f"[DDS][downlink] TrainCmd transit≈{lat_ms} ms (round={rid})")
                    # <<< 新增结束
                    self._process(samples.get_at(i))
            finally:
                reader.return_loan(samples, infos)
        except Exception:
            import traceback as _tb;
            _tb.print_exc()
    def _process(self, cmd):
        round_id = int(cmd.round_id); subset = int(cmd.subset_size)
        epochs = int(cmd.epochs); lr = float(cmd.lr); seed = int(cmd.seed)
        if round_id <= self.client.last_round: return
        self.client.last_round = round_id
        print(f"[Client] TrainCmd: round={round_id} subset={subset} epochs={epochs} lr={lr} seed={seed}")
        try:
            t0 = int(_time.time()*1000); tr = self.client.run_training(round_id, subset, epochs, lr, seed); t1 = int(_time.time()*1000)
            print(f"[Client] local train+pack cost: {t1 - t0} ms")
            if tr["mode"] == "stream":
                pkts: _List[bytes] = tr["packets"]; total = len(pkts)
                for i, pkt in enumerate(pkts):
                    upd = dds.ClientUpdate()
                    upd.client_id = Client.CLIENT_ID; upd.round_id = cmd.round_id
                    upd.num_samples = int(tr["num_samples"]) if (i == total - 1) else 0
                    upd.data = pkt; self.client.upd_writer.write(upd)
                print(f"[Client] sent stream packets: {total}")
            else:
                upd = dds.ClientUpdate()
                upd.client_id = Client.CLIENT_ID; upd.round_id = cmd.round_id
                upd.num_samples = int(tr["num_samples"]); upd.data = tr["bytes"]
                self.client.upd_writer.write(upd)
                print(f"[Client] sent single update bytes={len(tr['bytes'])}")
        except Exception:
            import traceback as _tb; _tb.print_exc()


class _ModelBlobListener(dds.DataReaderListener):
    def __init__(self, cli: Client):
        super().__init__(); self.client = cli

    @staticmethod
    def _latency_ms_from_info(info):
        try:
            st = getattr(info, "source_timestamp", None)
            if st is None:
                return None
            if hasattr(st, "sec") and hasattr(st, "nanosec"):
                send_ms = int(st.sec) * 1000 + int(st.nanosec) // 1_000_000
            elif isinstance(st, (tuple, list)) and len(st) >= 2:
                send_ms = int(st[0]) * 1000 + int(st[1]) // 1_000_000
            else:
                send_ms = int(float(st) * 1000.0)
            now_ms = int(_time.time() * 1000)
            return max(0, now_ms - send_ms)
        except Exception:
            return None

    def on_data_available(self, reader):
        try:
            samples = dds.ModelBlobSeq();
            infos = dds.SampleInfoSeq()
            reader.take(samples, infos, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
            try:
                for i in range(samples.length()):
                    if not infos.get_at(i).valid_data:
                        continue
                    # >>> 新增：打印 controller→client 的 ModelBlob 传输时间
                    lat_ms = self._latency_ms_from_info(infos.get_at(i))
                    if lat_ms is not None:
                        s = samples.get_at(i)
                        sz = 0 if (not hasattr(s, "data") or s.data is None) else len(s.data)
                        rid = int(getattr(s, "round_id", -1))
                        print(f"[DDS][downlink] ModelBlob transit≈{lat_ms} ms (round={rid} bytes={sz})")
                    # <<< 新增结束
                    self._process(samples.get_at(i))
            finally:
                reader.return_loan(samples, infos)
        except Exception:
            import traceback as _tb;
            _tb.print_exc()
    def _process(self, mb):
        try:
            buf = mb.data
            self.client.latest_model_dir.mkdir(parents=True, exist_ok=True)
            with open(self.client.latest_model_path, "wb") as f: f.write(buf)
            self.client.latest_model_round = int(mb.round_id)
            print(f"[Client] ModelBlob: round={mb.round_id} -> {self.client.latest_model_path.resolve()}")
        except Exception as e:
            print(f"[Client] failed to save ModelBlob: {e}", file=_sys.stderr)


if __name__ == "__main__":
    if len(_sys.argv) != 2:
        print("Usage: python Client_merged.py <client_merged.conf.json>"); _sys.exit(1)
    Client.main(_sys.argv[1:])
