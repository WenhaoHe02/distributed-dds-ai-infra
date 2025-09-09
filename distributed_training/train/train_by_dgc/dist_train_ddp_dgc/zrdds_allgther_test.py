# -*- coding: utf-8 -*-
import os, time, struct
import DDS_All as dds
from zrdds_allgather import ZrddsAllgather

def main():
    RANK  = int(os.environ.get("RANK", "0"))
    WORLD = int(os.environ.get("WORLD_SIZE", "2"))
    GROUP = os.environ.get("GROUP_ID", "test-group")
    DOMAIN_ID = int(os.environ.get("DDS_DOMAIN_ID", "200"))
    DISCOVERY_WARMUP_MS = int(os.environ.get("DISCOVERY_WARMUP_MS", "0"))  # 可选：用于验证竞态

    print(f"[env] rank={RANK} world={WORLD} group={GROUP} domain={DOMAIN_ID}")

    # ---- DDS 初始化
    print(f"[rank {RANK}] creating DomainParticipant...")
    dpf = dds.DomainParticipantFactory.get_instance()
    dp = dpf.create_participant(DOMAIN_ID, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
    if not dp:
        print(f"[rank {RANK}] ERROR: create_participant failed")
        return
    print(f"[rank {RANK}] DomainParticipant created: {dp}")

    print(f"[rank {RANK}] registering all types...")
    dds.register_all_types(dp)

    print(f"[rank {RANK}] creating ZrddsAllgather(topic=ddp/allgather_blob)")
    ag = ZrddsAllgather(dp, topic="ddp/allgather_blob")

    # ---- 仅用于观察发现是否完成（暖机期日志）
    # 由于我们在这个文件拿不到 writer/reader 的 matched 统计，这里只能打印时间窗口内的“等待发现完成”的提示
    if DISCOVERY_WARMUP_MS > 0:
        t0 = time.time()
        print(f"[rank {RANK}] discovery warmup for {DISCOVERY_WARMUP_MS} ms ...")
        time.sleep(DISCOVERY_WARMUP_MS / 1000.0)
        print(f"[rank {RANK}] warmup done in {int((time.time()-t0)*1000)} ms")

    # ---- 构造 payload（4 字节 rank）
    payload = struct.pack("<I", RANK)
    round_id = 12345
    name = "demo"

    print(f"[rank {RANK}] calling allgather_async, payload={list(payload)}")
    key, wait_fn = ag.allgather_async(
        group_id=GROUP, round_id=round_id, name=name, part_id=0,
        rank=RANK, world=WORLD, payload=payload
    )
    print(f"[rank {RANK}] allgather_async returned key={key}, wait_fn={wait_fn}")

    try:
        print(f"[rank {RANK}] waiting for frames...")
        frames = wait_fn(timeout=10000.0)
        print(f"[rank {RANK}] wait_fn returned {len(frames)} frames")
    except Exception as e:
        print(f"[rank {RANK}] allgather failed: {e}")
        return

    vals = [struct.unpack('<I', f)[0] for f in frames]
    print(f"[rank {RANK}] got values={vals}")

    print(f"[rank {RANK}] cleaning up...")
    dp.delete_contained_entities()
    print(f"[rank {RANK}] finished")

if __name__ == "__main__":
    main()
