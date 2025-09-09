# zrdds_allgather.py
import struct, threading, time
import DDS_All as dds
from collections import defaultdict

MAGIC = b'AG1\0'
# < little-endian: magic, gl, nl, part_id, rank, round_id, world, seq, seq_cnt, reserved
HDR_FMT = "<4sH H H H I H H H H"
HDR_SIZE = struct.calcsize(HDR_FMT)

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

class ZrddsAllgather:
    """
    通过单个 Topic 进行 allgather：
      - 每个 rank 把自己的分片广播出去
      - 每个 rank 同时收集所有 rank 的同名分片，收齐后返回
    """
    def __init__(self, dp, topic="ddp/allgather_blob", history_depth=10):
        self.dp = dp
        self.topic = dp.create_topic(topic, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)

        self.pub = dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        wq = dds.DataWriterQos(); self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = history_depth
        self.writer = self.pub.create_datawriter(self.topic, wq, None, 0)

        rq = dds.DataReaderQos(); self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS
        self.reader = self.sub.create_datareader(self.topic, rq, None, 0)

        self._buckets = defaultdict(lambda: defaultdict(list))  # key -> rank -> [(seq, bytes)...]
        self._done = {}
        self._lock = threading.Lock()

        class _L(dds.DataReaderListener):
            def __init__(self, outer): super().__init__(); self.o = outer
            def on_data_available(self, r):
                seq = dds.ModelBlobSeq(); info = dds.SampleInfoSeq()
                r.take(seq, info, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
                try:
                    for i in range(seq.length()):
                        if info.get_at(i).valid_data:
                            data = bytes(seq.get_at(i).data or b"")
                            self.o._on_raw(data)
                finally:
                    r.return_loan(seq, info)
        self.listener = _L(self)
        self.reader.set_listener(self.listener, dds.StatusKind.DATA_AVAILABLE_STATUS)

    @staticmethod
    def _make_key(group_id, round_id, name, part_id):
        return f"{group_id}|{round_id}|{name}|{part_id}"

    def _on_raw(self, raw: bytes):
        g, r, name, p, rank, world, seq, seq_cnt, payload = unpack_frame(raw)
        key = self._make_key(g, r, name, p)
        with self._lock:
            self._buckets[key][rank].append((seq, payload))
            # 该 rank 的分段齐了就拼接
            if len(self._buckets[key][rank]) == seq_cnt:
                self._buckets[key][rank].sort(key=lambda x: x[0])
                self._buckets[key][rank] = [b for _, b in self._buckets[key][rank]]
            # 所有 rank 齐了就置完成
            ranks = self._buckets[key]
            done = len(ranks) == world and all(isinstance(v, list) and len(v) > 0 for v in ranks.values())
            if done and key in self._done: self._done[key].set()

    def allgather_async(self, *, group_id:str, round_id:int, name:str, part_id:int,
                        rank:int, world:int, payload:bytes, max_chunk=1<<20):
        """ 发送一个 part 的 payload；返回 handle（等待并取回所有 rank 的该 part） """
        chunks = [payload[i:i+max_chunk] for i in range(0, len(payload or b""), max_chunk)] or [b""]
        key = self._make_key(group_id, round_id, name, part_id)
        with self._lock:
            if key not in self._done: self._done[key] = threading.Event()
        for seq, ck in enumerate(chunks):
            mb = dds.ModelBlob()
            body = pack_frame(group_id, round_id, name, part_id, rank, world, seq, len(chunks), ck)
            b = bytearray()
            b.extend(body)
            mb.data = body
            self.writer.write(mb)

        ev = self._done[key]
        def await_and_collect(timeout=None):
            ok = ev.wait(timeout)
            if not ok: raise TimeoutError(f"allgather timeout: {key}")
            with self._lock:
                mp = self._buckets.pop(key, {})
                self._done.pop(key, None)
            # 统一按 rank 排序 & 拼接
            return [b"".join(mp[r]) for r in sorted(mp.keys())]
        return (key, await_and_collect)

    @staticmethod
    def synchronize(handles, timeout=None):
        return [h[1](timeout) for h in handles]
