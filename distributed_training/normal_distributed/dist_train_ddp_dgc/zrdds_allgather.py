# -*- coding: utf-8 -*-
import logging
import struct, threading, time
import DDS_All as dds
from collections import defaultdict

MAGIC = b'AG1\0'
# < little-endian: magic, gl, nl, part_id, rank, round_id, world, seq, seq_cnt, reserved
HDR_FMT = "<4sH H H H I H H H H"
HDR_SIZE = struct.calcsize(HDR_FMT)

logging.basicConfig(level=logging.INFO)

def pack_frame(group_id, round_id, tensor_name, part_id, rank, world, seq, seq_cnt, payload: bytes):
    g = group_id.encode('utf-8'); n = tensor_name.encode('utf-8')
    hdr = struct.pack(HDR_FMT, MAGIC, len(g), len(n), part_id, rank, round_id, world, seq, seq_cnt, 0)
    return hdr + g + n + (payload or b"")

def unpack_frame(frame: bytes):
    magic, gl, nl, part_id, rank, round_id, world, seq, seq_cnt, _ = struct.unpack(HDR_FMT, frame[:HDR_SIZE])
    assert magic == MAGIC
    off = HDR_SIZE
    group_id = frame[off:off+gl].decode(); off += gl
    name = frame[off:off+nl].decode(); off += nl
    payload = frame[off:]
    return group_id, round_id, name, part_id, rank, world, seq, seq_cnt, payload

class _AGHandle:
    """
    句柄对象：将“等待”(wait, 纯通信时间, 单位=毫秒) 与 “收集合并”(collect) 分离。
    兼容旧接口(tuple)：
      - h[0] -> key
      - h[1] -> await_and_collect(timeout_ms)   # 注意：参数按“毫秒”解释（与旧实现一致）
    """
    __slots__ = ("key", "_ev", "_get_fn", "wait_ms")

    def __init__(self, key: str, ev: threading.Event, get_and_clear_fn):
        self.key = key
        self._ev = ev
        self._get_fn = get_and_clear_fn
        self.wait_ms = 0.0

    # 纯等待（毫秒）
    def wait(self, timeout_ms=None):
        t0 = time.perf_counter()
        ok = self._ev.wait(None if timeout_ms is None else (timeout_ms / 1000.0))
        self.wait_ms += (time.perf_counter() - t0) * 1000.0
        if not ok:
            raise TimeoutError(f"allgather timeout: {self.key}")
        return True

    # 收集合并（不计入等待时间）
    def collect(self):
        return self._get_fn()

    # ---- 旧 tuple 语义兼容：h[1](timeout_ms) ----
    def __getitem__(self, idx):
        if idx == 0:
            return self.key
        if idx == 1:
            def _await_and_collect(timeout_ms=None):
                # 与旧版保持一致：传入的数按“毫秒”处理
                self.wait(timeout_ms)
                return self.collect()
            return _await_and_collect
        raise IndexError("invalid handle index; use 0 for key or 1 for await_fn")

class ZrddsAllgather:
    """
    通过单个 Topic 进行 allgather：
      - 每个 rank 把自己的分片广播出去
      - 每个 rank 同时收集所有 rank 的同名分片，收齐后返回
    """
    def __init__(self, dp, topic="ddp/allgather_blob", history_depth=4, debug=True):
        self.dp = dp
        self.debug = bool(debug)

        # Topic
        self.topic = dp.create_topic(topic, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)
        logging.info(f"[ag] topic created: name={topic}, type=ModelBlob")

        # Pub/Sub
        self.pub = dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        # Writer：RELIABLE + KEEP_LAST(depth)
        wq = dds.DataWriterQos()
        self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS
        wq.history.depth = history_depth
        self.writer = self.pub.create_datawriter(self.topic, wq, None, 0)

        rq = dds.DataReaderQos()
        self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS


        # Buckets / state
        self._buckets = defaultdict(lambda: defaultdict(list))  # key -> rank -> [(seq, bytes)...]
        self._done = {}
        self._tx_bytes = 0      # 已发送（含头）
        self._rx_bytes = 0      # 已接收（含头）
        self._tx_payload = 0    # 仅载荷
        self._rx_payload = 0    # 仅载荷
        self._lock = threading.Lock()

        # Reader listener
        class _ReaderL(dds.DataReaderListener):
            def __init__(self, outer):
                super().__init__(); self.o = outer

            def on_subscription_matched(self, reader, status):
                # 这条是否触发取决于 DataReader 的 mask；我们不强依赖它
                if not self.o.debug: return
                try:
                    logging.info(f"[ag][match][reader] current={status.current_count} "
                                 f"change={status.current_count_change} total={status.total_count}")
                except Exception as e:
                    logging.info(f"[ag][match][reader] error printing status: {e}")

            def on_data_available(self, r):
                seq = dds.ModelBlobSeq()
                info = dds.SampleInfoSeq()
                r.take(seq, info, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
                try:
                    n = min(seq.length(), info.length())
                    for i in range(n):
                        inf = info.get_at(i)
                        if getattr(inf, "valid_data", False):
                            blob = seq.get_at(i)
                            try:
                                data = bytes(blob.data or b"")
                            except Exception:
                                data = bytes(blob.data)
                            self.o._on_raw(data)
                finally:
                    r.return_loan(seq, info)

        self.listener = _ReaderL(self)
        # ★ 为避免兼容性问题，这里只监听 DATA_AVAILABLE（旧版就是这么做的）
        self.reader = self.sub.create_datareader(self.topic, rq, self.listener,
                                                 dds.StatusKind.DATA_AVAILABLE_STATUS)

        logging.info(f"[ag] ZrddsAllgather ready: RELIABLE, writer KEEP_LAST depth={history_depth}, reader KEEP_LAST 4")

    @staticmethod
    def _make_key(group_id, round_id, name, part_id):
        return f"{group_id}|{round_id}|{name}|{part_id}"

    def bytes_counters(self):
        with self._lock:
            return self._tx_bytes, self._rx_bytes

    def payload_counters(self):
        with self._lock:
            return self._tx_payload, self._rx_payload

    def reset_bytes_counters(self):
        with self._lock:
            self._tx_bytes = 0
            self._rx_bytes = 0
            self._tx_payload = 0
            self._rx_payload = 0

    def _on_raw(self, raw: bytes):
        g, r, name, p, rank, world, seq, seq_cnt, payload = unpack_frame(raw)
        key = self._make_key(g, r, name, p)

        with self._lock:
            self._rx_bytes   += len(raw)        # 含头
            self._rx_payload += len(payload)    # 仅载荷
            self._buckets[key][rank].append((seq, payload))

        logging.info(f"[ag][recv] key={key} from_rank={rank} seg={seq + 1}/{seq_cnt} "
                     f"len={len(payload)} frame={len(raw)} world={world} total_rx={self._rx_bytes}")

        # rank 内分段齐了就拼接
        with self._lock:
            if len(self._buckets[key][rank]) == seq_cnt:
                self._buckets[key][rank].sort(key=lambda x: x[0])
                self._buckets[key][rank] = [b for _, b in self._buckets[key][rank]]
                total_len = sum(len(b) for b in self._buckets[key][rank])
                logging.info(f"[ag][recv] rank={rank} all segments arrived, merged_len={total_len}")

            # 所有 rank 齐了就置完成
            ranks = self._buckets[key]
            done = len(ranks) == world and all(isinstance(v, list) and len(v) > 0 for v in ranks.values())
            if done and key in self._done:
                logging.info(f"[ag][done] key={key} collected from ranks={sorted(ranks.keys())}, world={world}")
                self._done[key].set()

    def allgather_async(self, *, group_id:str, round_id:int, name:str, part_id:int,
                        rank:int, world:int, payload:bytes, max_chunk=4<<20):
        """发送一个 part 的 payload；返回 _AGHandle（wait 的单位=毫秒）"""
        chunks = [payload[i:i+max_chunk] for i in range(0, len(payload or b""), max_chunk)] or [b""]
        key = self._make_key(group_id, round_id, name, part_id)
        with self._lock:
            if key not in self._done:
                self._done[key] = threading.Event()

        # 发送分片
        for seq, ck in enumerate(chunks):
            mb = dds.ModelBlob()
            body = pack_frame(group_id, round_id, name, part_id, rank, world, seq, len(chunks), ck)
            mb.data = body
            self.writer.write(mb)
            with self._lock:
                self._tx_bytes   += len(body)  # 含头
                self._tx_payload += len(ck)    # 仅载荷
            logging.info(f"[ag][send] key={key} rank={rank} seg={seq+1}/{len(chunks)} "
                         f"len={len(ck)} (frame={len(body)}) total_tx={self._tx_bytes}")

        ev = self._done[key]

        def _get_and_clear():
            with self._lock:
                mp = self._buckets.pop(key, {})
                self._done.pop(key, None)
            out = [b"".join(mp[rk]) for rk in sorted(mp.keys())]
            sizes = [len(x) for x in out]
            logging.info(f"[ag][collect] key={key} done: ranks={sorted(mp.keys())} sizes={sizes}")
            return out

        return _AGHandle(key, ev, _get_and_clear)

    def wait_for_discovery(self, *, world: int, timeout_ms: int = 8000, include_self: bool = True, poll_ms: int = 100):
        deadline = time.time() + timeout_ms / 1000.0
        target = world if include_self else max(0, world - 1)

        def _get_pub_count():
            try:
                st = self.writer.get_publication_matched_status()
            except TypeError:
                st = dds.PublicationMatchedStatus()
                self.writer.get_publication_matched_status(st)
            return int(getattr(st, "current_count", 0))

        def _get_sub_count():
            try:
                st = self.reader.get_subscription_matched_status()
            except TypeError:
                st = dds.SubscriptionMatchedaStatus()
                self.reader.get_subscription_matched_status(st)
            return int(getattr(st, "current_count", 0))

        last_w = last_r = -1
        while True:
            cw = _get_pub_count()
            cr = _get_sub_count()
            if (cw >= target) and (cr >= target):
                logging.info(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}")
                return
            if cw != last_w or cr != last_r:
                logging.info(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}")
                last_w, last_r = cw, cr
            if time.time() >= deadline:
                raise TimeoutError(f"discovery timeout: writer={cw}, reader={cr}, target={target}, world={world}")
            time.sleep(poll_ms / 1000.0)

    @staticmethod
    def synchronize(handles, timeout=None):
        outs = []
        for h in handles:
            if hasattr(h, "wait"):
                h.wait(None if timeout is None else timeout)  # timeout 按毫秒解释（旧语义）
                outs.append(h.collect())
            else:
                outs.append(h[1](timeout))
        return outs
