import time
import struct
import DDS_All as dds

# ====== 工具：打印匹配状态======

def _wait_writer_matched(writer, min_matches, timeout_ms, tag="[barrier]"):
    start = time.time() * 1000.0
    last = -1
    ok = False
    while (time.time() * 1000.0 - start) < timeout_ms:
        try:
            st = writer.get_publication_matched_status()  # PublicationMatchedStatus
        except Exception as e:
            print(f"{tag} get_publication_matched_status error: {e}")
            break
        if st.current_count != last:
            print(f"{tag} writer matched: current={st.current_count} "
                  f"total={st.total_count} change={st.current_count_change}")
            last = st.current_count
        if st.current_count >= min_matches:
            ok = True
            break
        time.sleep(0.2)
    if not ok:
        print(f"{tag} writer matched NOT ready: need>={min_matches}, got={last}")
    return ok

def _wait_reader_matched(reader, min_matches, timeout_ms, tag="[barrier]"):
    start = time.time() * 1000.0
    last = -1
    ok = False
    while (time.time() * 1000.0 - start) < timeout_ms:
        try:
            st = reader.get_subscription_matched_status()  # SubscriptionMatchedStatus
        except Exception as e:
            print(f"{tag} get_subscription_matched_status error: {e}")
            break
        if st.current_count != last:
            print(f"{tag} reader matched: current={st.current_count} "
                  f"total={st.total_count} change={st.current_count_change}")
            last = st.current_count
        if st.current_count >= min_matches:
            ok = True
            break
        time.sleep(0.2)
    if not ok:
        print(f"{tag} reader matched NOT ready: need>={min_matches}, got={last}")
    return ok

# ====== barrier 本体 ======

def ddp_barrier_verbose(zrdds_allgather,
                        group_id: str, rank: int, world: int,
                        domain_id: int = None,
                        topic_name: str = "ddp/allgather_blob",
                        min_writer_matches: int = 1,
                        min_reader_matches: int = 1,
                        match_timeout_s: float = 15.0,
                        barrier_timeout_s: float = 60.0):

    print("========== [barrier] env ==========")
    print(f"[barrier] group_id={group_id} rank={rank} world={world} "
          f"domain_id={domain_id if domain_id is not None else 'N/A'} topic={topic_name}")

    # 1) 先确保 Writer/Reader 都有匹配（避免第一批包在对端未订阅时丢掉）
    print("========== [barrier] matching ==========")
    _ = _wait_reader_matched(zrdds_allgather.reader, min_reader_matches, int(match_timeout_s * 1000))
    _ = _wait_writer_matched(zrdds_allgather.writer, min_writer_matches, int(match_timeout_s * 1000))

    # 2) allgather barrier：每个 rank 发一个 4 字节 rank，收齐 world 份为止
    print("========== [barrier] allgather ==========")
    MAGIC_ROUND = 0x42615252  # 'BaRR'，与训练/评估的 round_id 空间隔离
    payload = struct.pack("<I", int(rank))

    # 发送并等待
    h = zrdds_allgather.allgather_async(group_id=group_id, round_id=MAGIC_ROUND,
                                        name="barrier", part_id=0,
                                        rank=rank, world=world, payload=payload)
    ok = True
    try:
        frames = h[1](barrier_timeout_s)  # list[bytes]（按 rank 排序）
        seen = [struct.unpack("<I", b)[0] for b in frames]
        print(f"[barrier] seen ranks: {seen}")
        missing = [i for i in range(world) if i not in seen]
        if missing:
            ok = False
            print(f"[barrier] MISSING ranks: {missing}")
    except TimeoutError as e:
        ok = False
        print(f"[barrier] TIMEOUT: {e}")

    print(f"========== [barrier] result: {'OK' if ok else 'FAILED'} ==========")
    return ok