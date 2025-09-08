package patcher;

import data_structure.*; // SingleTask / InferenceRequest / Task / TaskSeq / OpenBatch / Grant / TaskList

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TaskClassifier — per-model windowed batcher aligned to final ai.idl (Scheme B)
 *
 * Responsibilities
 *  - Accept InferenceRequest / SingleTask and aggregate into per-model windows.
 *  - When a window reaches thresholds (maxBatch or maxWaitMs), cut a batch:
 *      * create a new batch_id
 *      * snapshot TaskSeq and store internally (pendingBatches)
 *      * emit an OpenBatch{batch_id, model_id, size, create_ts_ms}
 *  - When Patcher later decides the winner via Grant{batch_id, winner_worker_id},
 *      * onGrant(...) will emit a TaskList{batch_id, model_id, assigned_worker_id, tasks}
 *
 * This class does NOT select a winner from Claims — that orchestration remains in Patcher.
 * It only: window → OpenBatch, and Grant → TaskList.
 */
public class TaskClassifier {

    /* -------------------- Wiring contracts -------------------- */
    /** Publish OpenBatch announcements. */
    public interface OpenBatchEmitter { void emit(OpenBatch ob); }
    /** Publish the final assigned TaskList after Grant. */
    public interface TaskListEmitter { void emit(TaskList tl); }

    /* -------------------- Config -------------------- */
    public static class Config {

        public int priorty=0;
        /** Default maximum batch size per model if no override. */
        public int defaultMaxBatch = 16;
        /** Default maximum wait per model (ms) if no override. */
        public long defaultMaxWaitMs = 8;
        /** Per-model overrides. Key = model_id (String). */
        public Map<String, Integer> perModelMaxBatch = new ConcurrentHashMap<>();
        public Map<String, Long> perModelMaxWaitMs = new ConcurrentHashMap<>();
        /** Per-window capacity to guard memory. Oldest items dropped if exceeded. */
        public int perWindowCapacity = 4096;
        /** Ticker period for closing time windows. */
        public long tickPeriodMs = 1L;
        /** Keep recently served batches this long for safety (ms). */
        public long servedRetentionMs = 10_000L;
    }

    /* -------------------- Internal types -------------------- */
    private static class PendingItem {
        final String requestId;
        final String taskId;
        final String clientId;
        final Bytes payload;
        PendingItem(String requestId, String taskId, String clientId, Bytes payload){
            this.requestId = requestId; this.taskId = taskId; this.clientId = clientId; this.payload = payload;
        }
        Task toTask(){
            Task t = new Task();
            t.request_id = requestId; t.task_id = taskId; t.client_id = clientId; t.payload = payload;
            return t;
        }
    }

    private static class Window {
        final String modelId;
        final ArrayDeque<PendingItem> q;
        long firstArrivedAt = 0L;
        Window(String modelId, int capacity){ this.modelId = modelId; this.q = new ArrayDeque<>(capacity); }
        int size(){ return q.size(); }
        boolean isEmpty(){ return q.isEmpty(); }
        void add(PendingItem pi){ if (q.isEmpty()) firstArrivedAt = System.currentTimeMillis(); q.addLast(pi); }
        List<PendingItem> drainUpTo(int n){
            List<PendingItem> out = new ArrayList<>(Math.min(n, q.size()));
            while(n-- > 0 && !q.isEmpty()) out.add(q.removeFirst());
            if (q.isEmpty()) firstArrivedAt = 0L; return out;
        }
        long ageMs(){ return firstArrivedAt==0?0:System.currentTimeMillis()-firstArrivedAt; }
    }

    private static class StoredBatch {
        final String batchId;
        final String modelId;
        final long createTs;
        final TaskSeq tasks;
        volatile String assignedWorkerId = null;
        StoredBatch(String batchId, String modelId, long createTs, TaskSeq tasks){
            this.batchId=batchId; this.modelId=modelId; this.createTs=createTs; this.tasks=tasks;
        }
    }

    /* -------------------- State -------------------- */
    private final Config cfg;
    private final OpenBatchEmitter openBatchEmitter;
    private final TaskListEmitter taskListEmitter;

    private final ConcurrentMap<String, Window> windows = new ConcurrentHashMap<>();                // model_id -> window
    private final ConcurrentMap<String, StoredBatch> pendingBatches = new ConcurrentHashMap<>();     // batch_id -> stored
    private final Deque<StoredBatch> servedRecently = new ArrayDeque<>();                            // for retention cleanup

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "taskclassifier-ticker"); t.setDaemon(true); return t; });
    private final AtomicLong batchSeq = new AtomicLong(0);

    public TaskClassifier(Config cfg, OpenBatchEmitter openBatchEmitter, TaskListEmitter taskListEmitter){
        this.cfg = cfg;
        this.openBatchEmitter = openBatchEmitter;
        this.taskListEmitter = taskListEmitter;
        long p = Math.max(1L, cfg.tickPeriodMs);
        scheduler.scheduleAtFixedRate(this::tick, p, p, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        scheduler.shutdownNow();
        windows.clear();
        pendingBatches.clear();
        servedRecently.clear();
    }

    /* -------------------- Public API -------------------- */
    /** Offer a full request (fan-out items into per-model windows). */
    public void offer(InferenceRequest req){
        if (req == null || req.tasks == null || req.tasks.length() == 0) return;

        cfg.priorty = extractPriority(req.request_id);

        for (int i = 0; i < req.tasks.length(); i++) {
            SingleTask st = req.tasks.get_at(i);
            if (st == null) continue;
            offer(st);
        }
    }

    /** Offer a single task (already contains model_id). */
    public void offer(SingleTask st){
        if (st == null || st.payload == null) return;
        Window w = windows.computeIfAbsent(st.model_id, mid -> new Window(mid, cfg.perWindowCapacity));
        if (w.q.size() >= cfg.perWindowCapacity) { w.q.pollFirst(); } // simple backpressure: drop oldest
        w.add(new PendingItem(st.request_id, st.task_id, st.client_id, st.payload));
        int maxBatch = cfg.perModelMaxBatch.getOrDefault(st.model_id, cfg.defaultMaxBatch);
        if (w.size() >= maxBatch) cutAndAnnounce(w, maxBatch);
    }

    /** When Patcher decided a winner, call this to emit the assigned TaskList. */
    public void onGrant(Grant grant){
        if (grant == null) return;
        StoredBatch sb = pendingBatches.remove(grant.batch_id);
        if (sb == null) return; // already served or unknown
        sb.assignedWorkerId = grant.winner_worker_id;
        emitTaskList(sb);
        servedRecently.addLast(sb);
        cleanupServed();
    }



    /* -------------------- Internal -------------------- */
    private void tick(){
        long now = System.currentTimeMillis();
        for (Window w : windows.values()){
            if (w.isEmpty()) continue;
            int maxBatch = cfg.perModelMaxBatch.getOrDefault(w.modelId, cfg.defaultMaxBatch);
            long maxWait = cfg.perModelMaxWaitMs.getOrDefault(w.modelId, cfg.defaultMaxWaitMs);
            if (w.size() >= maxBatch || w.ageMs() >= maxWait){
                cutAndAnnounce(w, maxBatch);
            }
        }
        cleanupServed();
    }

    private void cutAndAnnounce(Window w, int maxBatch){
        List<PendingItem> items = w.drainUpTo(maxBatch);
        if (items.isEmpty()) return;
        String batchId = newBatchId();
        TaskSeq seq = new TaskSeq();
        seq.ensure_length(items.size(), items.size());
        for (int i=0;i<items.size();i++) seq.set_at(i, items.get(i).toTask());
        StoredBatch sb = new StoredBatch(batchId, w.modelId, System.currentTimeMillis(), seq);
        pendingBatches.put(batchId, sb);

        OpenBatch ob = new OpenBatch();
        ob.batch_id = batchId;
        ob.model_id = w.modelId;
        ob.size = items.size();
        ob.create_ts_ms = (int)sb.createTs;
        try { openBatchEmitter.emit(ob); } catch (Exception e){ e.printStackTrace(); }
    }

    private void emitTaskList(StoredBatch sb){
        TaskList tl = new TaskList();
        tl.batch_id = sb.batchId;
        tl.model_id = sb.modelId;
        tl.assigned_worker_id = sb.assignedWorkerId;
        tl.tasks = sb.tasks;
        try { taskListEmitter.emit(tl); } catch (Exception e){ e.printStackTrace(); }
    }

    private void cleanupServed(){
        long now = System.currentTimeMillis();
        while(!servedRecently.isEmpty()){
            StoredBatch head = servedRecently.peekFirst();
            if (head == null) break;
            if (now - head.createTs > cfg.servedRetentionMs) servedRecently.pollFirst(); else break;
        }
    }

    private String newBatchId(){
        return "b-" + System.currentTimeMillis() + "-" + batchSeq.incrementAndGet();
    }

    public int extractPriority(String request_id){
        if (request_id == null) return 0;
        int idx = request_id.indexOf("priority:");
        if (idx < 0) return 0;
        try {
            return Integer.parseInt(request_id.substring(idx + 9).trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
