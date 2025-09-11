# improved_dds_barrier.py
import time
import struct
import threading
import traceback
from typing import Dict, List, Optional, Tuple, Callable
from collections import defaultdict
from enum import Enum

import DDS_All as dds
from sympy.strategies.core import switch


class NodeRole(Enum):
    CONTROLLER = "controller"
    CLIENT = "client"


# ====== 改进的 AllGather 实现 ======

MAGIC = b'AG1\0'
HDR_FMT = "<4s H H H H H H"
HDR_SIZE = struct.calcsize(HDR_FMT)


def pack_frame(tensor_name: str, cid: int, world: int, seq: int, seq_cnt: int, payload: bytes) -> bytes:
    n = tensor_name.encode('utf-8')
    hdr = struct.pack(HDR_FMT, MAGIC, len(n), cid, world, seq, seq_cnt, 0)
    return hdr + n + (payload or b"")


def unpack_frame(frame: bytes) -> Tuple[str, int, int, int, int, bytes]:
    magic, nl, cid, world, seq, seq_cnt, _ = struct.unpack(HDR_FMT, frame[:HDR_SIZE])
    assert magic == MAGIC, f"Invalid magic: {magic}"
    off = HDR_SIZE
    name = frame[off:off + nl].decode('utf-8')
    off += nl
    payload = frame[off:]
    return name, cid, world, seq, seq_cnt, payload


class ImprovedZrddsAllgather:
    """改进的 AllGather 实现，修复了线程安全、内存泄漏等问题"""

    def __init__(self, dp, cid,type:NodeRole,topic="ddp/allgather_blob", history_depth=10, debug=True,):
        self.dp = dp
        self.debug = debug
        self.self_cid=cid
        self.type=type
        self._done=False

        # 创建topic
        self.contro2clien_topic = dp.create_topic(topic+"contro2clien", "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)
        self.clien2contro_topic = dp.create_topic(topic+"clien2contro", "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)
        if self.debug:
            print(f"[ag] topic created: name={topic}, type=ModelBlob")

        # 创建publisher和subscriber
        self.pub = dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
        self.sub = dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

        # Writer配置
        wq = dds.DataWriterQos()
        self.pub.get_default_datawriter_qos(wq)
        wq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        wq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        wq.history.depth = history_depth
        if self.type==NodeRole.CLIENT:
            self.writer = self.pub.create_datawriter(self.clien2contro_topic, wq, None, 0)
        else:
            self.writer = self.pub.create_datawriter(self.contro2clien_topic, wq, None, 0)

        # Reader配置
        rq = dds.DataReaderQos()
        self.sub.get_default_datareader_qos(rq)
        rq.reliability.kind = dds.ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS
        rq.history.kind = dds.HistoryQosPolicyKind.KEEP_LAST_HISTORY_QOS
        rq.history.depth = 32

        # 数据结构
        self._buckets = defaultdict(list)  # key -> rank -> [(seq, bytes)...]
        self._completed_keys = defaultdict(list)  # key -> [merged_data_by_rank]
        self._lock = threading.RLock()  # 使用可重入锁

        # 清理相关
        self._cleanup_timer = None
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5分钟清理一次

        # 创建listener和reader
        self.listener = self._create_listener()
        mask = dds.StatusKind.DATA_AVAILABLE_STATUS
        if self.debug:
            mask = mask | dds.StatusKind.SUBSCRIPTION_MATCHED_STATUS

        if self.type==NodeRole.CLIENT:
            self.reader = self.sub.create_datareader(self.contro2clien_topic, rq, self.listener, mask)
        else:
            self.reader = self.sub.create_datareader(self.clien2contro_topic, rq, self.listener, mask)

        if self.debug:
            print(f"[ag] ImprovedZrddsAllgather ready: RELIABLE, depth={history_depth}")

    def _create_listener(self):
        """创建数据监听器"""

        class ReaderListener(dds.DataReaderListener):
            def __init__(self, outer):
                super().__init__()
                self.outer = outer

            def on_subscription_matched(self, reader, status):
                if self.outer.debug:
                    try:
                        print(f"[ag][match] reader matched: current={status.current_count} "
                              f"change={status.current_count_change} total={status.total_count}")
                    except Exception as e:
                        print(f"[ag][match] error printing status: {e}")

            def on_data_available(self, reader):
                seq = dds.ModelBlobSeq()
                info = dds.SampleInfoSeq()
                try:
                    reader.take(seq, info, -1, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
                    n_data = seq.length()
                    n_info = info.length()
                    n = min(n_data, n_info)

                    if n_data != n_info and self.outer.debug:
                        print(f"[ag][warn] data/info length mismatch: data={n_data}, info={n_info}")

                    for i in range(n):
                        inf = info.get_at(i)
                        if getattr(inf, "valid_data", False):
                            blob = seq.get_at(i)
                            try:
                                data = bytes(blob.data or b"")
                                self.outer._on_raw_data(data)
                            except Exception as e:
                                print(f"[ag][error] processing data failed: {e}")
                                if self.outer.debug:
                                    traceback.print_exc()
                finally:
                    reader.return_loan(seq, info)

                # 定期清理
                self.outer._maybe_cleanup()

        return ReaderListener(self)

    @staticmethod
    def _make_key(name: str, part_id: int) -> str:
        return f"{name}|{part_id}"

    def _validate_segments(self, segments: List[Tuple[int, bytes]], expected_count: int) -> bool:
        """验证分片的完整性和连续性"""
        if len(segments) != expected_count:
            return False

        segments.sort(key=lambda x: x[0])
        expected_seqs = list(range(expected_count))
        actual_seqs = [s[0] for s in segments]
        return actual_seqs == expected_seqs

    def _on_raw_data(self, raw: bytes):
        """处理接收到的原始数据"""
        try:
            name, cid, world, seq, seq_cnt, payload = unpack_frame(raw)
            print(name,cid,world,seq,seq_cnt,payload)
            key = self._make_key(name, cid)

            if self.type==NodeRole.CLIENT:
                print("get connection with Controller")
                self._done=True
            else:
                if self.debug:
                    print(f"[ag][recv] key={key} from cid={cid} seg={seq + 1}/{seq_cnt} "
                          f"len={len(payload)} world={world}")

                event_to_set = None

                with self._lock:
                    # 添加分片
                    self._buckets[key].append((seq, payload))

                    # 检查该rank的分片是否完整
                    if len(self._buckets[key]) == seq_cnt:
                        segments = self._buckets[key]

                        # 验证分片完整性
                        if not self._validate_segments(segments, seq_cnt):
                            print(f"[ag][error] invalid segments for {key}/rank={cid}")
                            return

                        if self.debug:
                            print(f"[ag][merged] rank={cid} segments merged, total_len={len(self._buckets[key])}")

                    # 检查是否所有rank都完成
                    ranks_data = self._buckets
                    print(ranks_data)

                    if len(ranks_data) == world:

                        # 清理buckets
                        del self._buckets[key]

                        if self.debug:
                            print(f"[ag][done] key={key} completed")
                        self._done=True

        except Exception as e:
            print(f"[ag][error] _on_raw_data failed: {e}")
            if self.debug:
                traceback.print_exc()

    def _maybe_cleanup(self):
        """定期清理过期数据"""
        now = time.time()
        if now - self._last_cleanup > self._cleanup_interval:
            self._cleanup_expired_data()
            self._last_cleanup = now

    def _cleanup_expired_data(self, max_age: float = 600):  # 10分钟
        """清理过期的未完成数据"""
        cutoff = time.time() - max_age
        with self._lock:
            # 这里可以根据实际需求添加时间戳来清理过期数据
            # 当前实现保持简单，只在明确完成时清理
            pass

    def allgather_async(self, *, name: str, cid: int, world: int, payload: bytes,
                        max_chunk: int = 1 << 20) -> Tuple[str, Callable]:
        """异步启动allgather操作"""
        chunks = [payload[i:i + max_chunk] for i in range(0, len(payload or b""), max_chunk)] or [b""]
        key = self._make_key(name, cid)

        # 发送所有分片
        for seq, chunk in enumerate(chunks):
            mb = dds.ModelBlob()
            frame_data = pack_frame(name, cid, world, seq, len(chunks), chunk)
            mb.data = frame_data

            ret = self.writer.write(mb)
            if self.debug:
                print(f"[ag][send] key={key} cid={cid} seg={seq + 1}/{len(chunks)} len={len(chunk)} ret={ret}")

        def await_and_collect(timeout: Optional[float] = None, poll_interval: float = 0.01) -> List[bytes]:
            """轮询等待 self._done 变为 True 并收集数据"""
            if self.debug:
                print(f"[ag][wait] key={key} waiting up to {timeout} s")

            start_time = time.time()
            while True:
                with self._lock:
                    if self._done:  # 只检查布尔变量
                        # 拿到数据并清理
                        mp = self._buckets.pop(key, {})
                        self._done = False  # 重置状态

                        # 按 rank 排序 & 拼接
                        out = [b"".join(mp[r]) for r in sorted(mp.keys())]
                        if self.debug:
                            sizes = [len(x) for x in out]
                            ranks = sorted(mp.keys())
                            print(f"[ag][wait] key={key} done: ranks={ranks} sizes={sizes}")
                        return out

                # 检查超时
                if timeout is not None and (time.time() - start_time) > timeout:
                    raise TimeoutError(f"allgather timeout: {key}")

                # 等待下一轮轮询
                time.sleep(poll_interval)

        return key, await_and_collect

    def _cleanup_key(self, key: str):
        """清理指定key的所有数据"""
        with self._lock:
            self._buckets.pop(key, None)
            self._completed_keys.pop(key, None)

    def wait_for_discovery(self, *, world: int, timeout_s: float = 30, poll_interval_s: float = 0.5):
        """等待DDS发现完成"""
        deadline = time.time() + timeout_s
        # 期望匹配数 = world - 1 (不包括自己)
        target_matches = max(0, world)

        def get_writer_matches() -> int:
            try:
                status = self.writer.get_publication_matched_status()
                return getattr(status, 'current_count', 0)
            except Exception as e:
                if self.debug:
                    print(f"[ag][discovery] get writer matches error: {e}")
                return 0

        def get_reader_matches() -> int:
            try:
                status = self.reader.get_subscription_matched_status()
                return getattr(status, 'current_count', 0)
            except Exception as e:
                if self.debug:
                    print(f"[ag][discovery] get reader matches error: {e}")
                return 0

        last_writer, last_reader = -1, -1

        while time.time() < deadline:
            writer_matches = get_writer_matches()
            reader_matches = get_reader_matches()

            if writer_matches >= target_matches and reader_matches >= target_matches:
                if self.debug:
                    print(
                        f"[ag][discovery] SUCCESS: writer={writer_matches}, reader={reader_matches}, target={target_matches}")
                return True

            # 状态变化时打印
            if writer_matches != last_writer or reader_matches != last_reader:
                if self.debug:
                    print(
                        f"[ag][discovery] progress: writer={writer_matches}, reader={reader_matches}, target={target_matches}")
                last_writer, last_reader = writer_matches, reader_matches

            time.sleep(poll_interval_s)

        # 超时
        writer_matches = get_writer_matches()
        reader_matches = get_reader_matches()
        raise TimeoutError(
            f"Discovery timeout: writer={writer_matches}, reader={reader_matches}, target={target_matches}")

    def cleanup(self):
        """清理所有资源"""
        with self._lock:
            # 设置所有等待的事件

            self._buckets.clear()
            self._completed_keys.clear()


# ====== 连接状态检查和Barrier ======

class ConnectionManager:
    """管理多client + controller的连接状态"""

    def __init__(self, node_role: NodeRole, node_id: int, world_size: int,
                 allgather: ImprovedZrddsAllgather, debug: bool = True):
        self.node_role = node_role
        self.node_id = node_id
        self.world_size = world_size
        self.allgather = allgather
        self.debug = debug

        # 连接状态
        self.is_connected = False
        self.connected_nodes = set()
        self._connection_lock = threading.Lock()

    def wait_for_all_connections(self, timeout_s: float = 60) -> bool:
        """等待所有节点连接完成"""
        if self.debug:
            print(f"[conn] {self.node_role.value}({self.node_id}) waiting for all connections...")
            print(f"[conn] world_size={self.world_size}, expecting {self.world_size - 1} other nodes")

        try:
            # 1. 等待DDS发现
            print("========== [barrier] matching ==========")
            self.allgather.wait_for_discovery(world=self.world_size, timeout_s=timeout_s)

            # 2. 执行连接确认barrier
            print("========== [barrier] allgather ==========")
            self._perform_connection_barrier(timeout_s)

            with self._connection_lock:
                self.is_connected = True

            if self.debug:
                print(f"[conn] {self.node_role.value}({self.node_id}) all connections established!")
            return True

        except Exception as e:
            print(f"[conn] {self.node_role.value}({self.node_id}) connection failed: {e}")
            if self.debug:
                traceback.print_exc()
            return False

    def _perform_connection_barrier(self, timeout_s: float):
        """执行连接确认barrier"""
        if self.debug:
            print(f"[conn] {self.node_role.value}({self.node_id}) starting connection barrier...")

        # 构造节点信息
        node_info = struct.pack("<II16s",
                                self.node_id,
                                1 if self.node_role == NodeRole.CONTROLLER else 0,
                                self.node_role.value.encode('utf-8')[:16].ljust(16, b'\x00'))

        # 执行allgather
        handle = self.allgather.allgather_async(
            name="connection_barrier",
            cid=self.node_id,
            world=self.world_size,
            payload=node_info
        )

        # 等待结果
        try:
            results = handle[1](timeout_s)
            #self._validate_connection_results(results)
        except TimeoutError:
            raise TimeoutError("Connection barrier timeout")

    def _validate_connection_results(self,results: List[bytes]):
        """验证连接结果"""
        connected_nodes = set()
        controller_count = 0

        for i, data in enumerate(results):
            if len(data) < 8:
                raise RuntimeError(f"Invalid data from rank {i}")

            node_id, is_controller = struct.unpack("<II", data[:8])
            role_bytes = data[8:24]
            role = role_bytes.rstrip(b'\x00').decode('utf-8')

            connected_nodes.add(node_id)
            if is_controller:
                controller_count += 1

            if self.debug:
                print(f"[conn] discovered node: id={node_id}, role={role}, is_controller={bool(is_controller)}")

        # 验证拓扑结构
        if controller_count != 1:
            raise RuntimeError(f"Expected exactly 1 controller, found {controller_count}")

        expected_nodes = set(range(self.world_size))
        if connected_nodes != expected_nodes:
            missing = expected_nodes - connected_nodes
            extra = connected_nodes - expected_nodes
            raise RuntimeError(f"Node set mismatch. Missing: {missing}, Extra: {extra}")

        with self._connection_lock:
            self.connected_nodes = connected_nodes

        if self.debug:
            print(f"[conn] barrier completed successfully, {len(connected_nodes)} nodes connected")

    def is_ready_for_training(self) -> bool:
        """检查是否准备好进行训练"""
        with self._connection_lock:
            return self.is_connected and len(self.connected_nodes) == self.world_size


def create_barrier_system(node_role: NodeRole,type:NodeRole, node_id: int, world_size: int,
                          domain_participant, topic_name: str = "ddp/barrier_topic",
                          debug: bool = True) -> Tuple[ImprovedZrddsAllgather, ConnectionManager]:
    """创建完整的barrier系统"""

    # 创建改进的allgather
    allgather = ImprovedZrddsAllgather(
        dp=domain_participant,
        topic=topic_name,
        cid=node_id,
        type=type,
        debug=debug
    )

    # 创建连接管理器
    conn_manager = ConnectionManager(
        node_role=node_role,
        node_id=node_id,
        world_size=world_size,
        allgather=allgather,
        debug=debug
    )

    return allgather, conn_manager

