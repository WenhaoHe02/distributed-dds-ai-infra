# ZrddsDenseBroadcast.py
# -*- coding: utf-8 -*-
import struct, threading, time, traceback
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

class ZrddsDenseBroadcast:
    """
    通过单个 Topic 进行 allgather：
      - 每个 rank 把自己的分片广播出去
      - 每个 rank 同时收集所有 rank 的同名分片，收齐后返回
    """
    def __init__(self, dp, topic="ddp/allgather_blob", history_depth=10, debug=True):
        self.dp = dp
        self.debug = debug

        # Topic
        self.topic = dp.create_topic(topic, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)
        if self.debug:
            print(f"[ag] topic created: name={topic}, type=ModelBlob")

        # Pub/Sub
        self.pub = dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        # Writer：RELIABLE + KEEP_LAST(depth)，不挂 listener（避免 native 回调崩）
        wq = dds.DataWriterQos();
        self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = history_depth
        self.writer = self.pub.create_datawriter(self.topic, wq, None, 0)

        # Reader：RELIABLE + KEEP_LAST(32) 更稳（除非你必须保所有历史）
        rq = dds.DataReaderQos()
        self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        rq.history.depth = 32


        # Buckets / state
        self._buckets = defaultdict(lambda: defaultdict(list))  # key -> rank -> [(seq, bytes)...]
        self._done = {}
        self._lock = threading.Lock()

        # [DEBUG] Reader listener: 打印 subscription matched + 数据到达
        class _ReaderL(dds.DataReaderListener):
            def __init__(self, outer):
                super().__init__(); self.o = outer

            # 订阅匹配事件
            def on_subscription_matched(self, reader, status):
                try:
                    print(f"[ag][match][reader] current={status.current_count} "
                          f"change={status.current_count_change} total={status.total_count}")
                except Exception as e:
                    print(f"[ag][match][reader] error printing status: {e}")

            # 数据可读事件
            def on_data_available(self, r):
                seq = dds.ModelBlobSeq();
                info = dds.SampleInfoSeq()
                r.take(seq, info, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
                try:
                    n_data = seq.length()
                    n_info = info.length()
                    n = min(n_data, n_info)
                    # [DEBUG] 观察长度是否不一致
                    # print(f"[ag][recv] loaned data={n_data}, info={n_info}, iter={n}")

                    for i in range(n):
                        inf = info.get_at(i)
                        if getattr(inf, "valid_data", False):
                            blob = seq.get_at(i)
                            try:
                                data = bytes(blob.data or b"")
                            except Exception:
                                data = bytes(blob.data)  # 兜底
                            self.o._on_raw_dense(data)
                    # 如果确实不一致，打个日志方便定位
                    if n_data != n_info:
                        print(f"[ag][warn] data/info length mismatch: data={n_data}, info={n_info}")
                finally:
                    r.return_loan(seq, info)

        self.listener = _ReaderL(self)
        mask = dds.StatusKind.DATA_AVAILABLE_STATUS
        if self.debug:
            mask = mask | dds.StatusKind.SUBSCRIPTION_MATCHED_STATUS
        self.reader = self.sub.create_datareader(
            self.topic, rq, self.listener, dds.StatusKind.DATA_AVAILABLE_STATUS
        )

        if self.debug:
            print("[ag] ZrddsAllgather ready: RELIABLE, writer KEEP_LAST depth="
                  f"{history_depth}, reader KEEP_ALL")

    @staticmethod
    def _make_key(group_id, round_id, name, part_id):
        return f"{group_id}|{round_id}|{name}|{part_id}"

    def _on_raw_dense(self, raw: bytes):
        """
        处理收到的稠密 int8 数据帧，直接广播收集到 _buckets
        """
        # 解析帧
        g, r, name, p, rank, world, seq, seq_cnt, payload = unpack_frame(raw)
        key = self._make_key(g, r, name, p)

        if self.debug:
            print(f"[dense][recv] key={key} from_rank={rank} len={len(payload)} world={world}")

        with self._lock:
            # 直接存储 payload，无需分片拼接
            self._buckets[key][rank] = payload

            # 检查是否所有 rank 都收齐
            ranks = self._buckets[key]
            done = len(ranks) == world and all(isinstance(v, (bytes, bytearray)) and len(v) > 0 for v in ranks.values())
            if done:
                if self.debug:
                    have = sorted(ranks.keys())
                    sizes = [len(ranks[r]) for r in have]
                    print(f"[dense][done] key={key} collected from ranks={have}, sizes={sizes}, world={world}")
                if key in self._done:
                    self._done[key].set()

    def allgather_async(self, *, group_id:str, round_id:int, name:str, part_id:int,
                        rank:int, world:int, payload:bytes, max_chunk=1<<20):
        """发送一个 part 的 payload；返回 handle（等待并取回所有 rank 的该 part）"""
        chunks = [payload[i:i+max_chunk] for i in range(0, len(payload or b""), max_chunk)] or [b""]
        key = self._make_key(group_id, round_id, name, part_id)
        with self._lock:
            if key not in self._done:
                self._done[key] = threading.Event()

        # 发送分片
        for seq, ck in enumerate(chunks):
            mb = dds.ModelBlob()
            body = pack_frame(group_id, round_id, name, part_id, rank, world, seq, len(chunks), ck)

            # 用 bytearray / memoryview 显式拷贝，规避某些绑定对 bytes 的坑
            mb.data = body

            self.writer.write(mb)
            if self.debug:
                print(f"[ag][send] key={key} rank={rank} seg={seq + 1}/{len(chunks)} len={len(ck)}")

        ev = self._done[key]

        def await_and_collect(timeout=None):
            if self.debug:
                print(f"[ag][wait] key={key} waiting up to {timeout} ms")
            ok = ev.wait(None if timeout is None else (timeout/1000.0))
            if not ok:
                raise TimeoutError(f"allgather timeout: {key}")
            with self._lock:
                mp = self._buckets.pop(key, {})
                self._done.pop(key, None)
            # 统一按 rank 排序 & 拼接
            out = [mp[r] for r in sorted(mp.keys())]
            if self.debug:
                sizes = [len(x) for x in out]
                ranks = sorted(mp.keys())
                print(f"[ag][wait] key={key} done: ranks={ranks} sizes={sizes}")
            return out

        return (key, await_and_collect)

    def wait_for_discovery(self, *, world: int, timeout_ms: int = 8000, include_self: bool = True, poll_ms: int = 100):
        """
        轮询等待 DDS 匹配完成：
          - world: 期望总参与数（rank 数）
          - include_self=True 时，要求匹配数 >= world；False 时要求 >= world-1
          - timeout_ms 超时抛出 TimeoutError
        同时检查 writer 的 publication_matched 和 reader 的 subscription_matched
        """
        deadline = time.time() + timeout_ms / 1000.0
        target = world if include_self else max(0, world - 1)

        def _get_pub_count():
            # 兼容两种绑定风格：返回 struct 或者通过 out-param 填充
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
                st = dds.SubscriptionMatchedStatus()
                self.reader.get_subscription_matched_status(st)
            return int(getattr(st, "current_count", 0))

        last_w = last_r = -1
        while True:
            cw = _get_pub_count()
            cr = _get_sub_count()
            if (cw >= target) and (cr >= target):
                print(f"[ag][discovery] OK: writer={cw}, reader={cr}, target={target}")
                return
            # 变化时打印一条
            if cw != last_w or cr != last_r:
                print(f"[ag][discovery] waiting... writer={cw}, reader={cr}, target={target}")
                last_w, last_r = cw, cr
            if time.time() >= deadline:
                raise TimeoutError(f"discovery timeout: writer={cw}, reader={cr}, target={target}, world={world}")
            time.sleep(poll_ms / 1000.0)

    @staticmethod
    def synchronize(handles, timeout=None):
        return [h[1](timeout) for h in handles]
