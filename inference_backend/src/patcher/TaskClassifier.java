package patcher;

import data_structure.*; // SingleTask / InferenceRequest / Task / TaskSeq / OpenBatch / Grant / TaskList
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TaskClassifier — per-(model_id, priority) windowed batcher (no IDL change)
 *
 * - Priority is parsed from InferenceRequest.request_id, e.g. "... priority:0".
 * - Windows are keyed by (model_id, priority). Different priorities never batch together.
 * - OpenBatch is unchanged; we encode priority into batch_id: "b-p{p}-{ts}-{seq}".
 * - Patcher will parse p from batch_id.
 */
public class TaskClassifier {

    /* -------------------- Wiring contracts -------------------- */
    public interface OpenBatchEmitter { void emit(OpenBatch ob); }
    public interface TaskListEmitter  { void emit(TaskList tl); }

    /* -------------------- Config -------------------- */
    public static class Config {
        public int  defaultMaxCpuBatch = 8;  // used for p=2
        public int  defaultMaxGpuBatch = 4;  // used for p=0
        public long defaultMaxWaitMs   = 8;  // used for p=2
        public long waitFastMs         = 4;  // used for p=0 (very short)
        public long waitNoneMs         = 0;  // used for p=1 (no wait)

        public Map<String, Integer> perModelMaxBatch  = new ConcurrentHashMap<>();
        public Map<String, Long>    perModelMaxWaitMs = new ConcurrentHashMap<>();

        public int  perWindowCapacity = 256;
        public long tickPeriodMs      = 1L;
        public long servedRetentionMs = 10_000L;
    }

    private static final int DEFAULT_PRIORITY = 2; // 0=GPU快, 1=不等, 2=默认（建议默认 2，可按需改为 0）

    /* -------------------- Internal types -------------------- */
    private static class PendingItem {
        final String requestId, taskId, clientId; final Bytes payload;
        PendingItem(String requestId, String taskId, String clientId, Bytes payload){
            this.requestId=requestId; this.taskId=taskId; this.clientId=clientId; this.payload=payload;
        }
        Task toTask(){
            Task t = new Task();
            t.request_id=requestId; t.task_id=taskId; t.client_id=clientId; t.payload=payload;
            return t;
        }
    }

    /** Window key = (modelId, priority) */
    private static final class Key {
        final String modelId; final int priority;
        Key(String m, int p){ this.modelId=m; this.priority=p; }
        @Override public boolean equals(Object o){
            if (this==o) return true;
            if (!(o instanceof Key)) return false;
            Key k=(Key)o; return priority==k.priority && Objects.equals(modelId, k.modelId);
        }
        @Override public int hashCode(){ return Objects.hash(modelId, priority); }
    }

    private static class Window {
        final String modelId; final int priority;
        final ConcurrentLinkedDeque<PendingItem> q = new ConcurrentLinkedDeque<>();
        long firstArrivedAt = 0L;
        Window(String modelId, int priority){ this.modelId=modelId; this.priority=priority; }
        int  size(){ return q.size(); }
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
        final String batchId, modelId; final long createTs; final TaskSeq tasks; final int priority;
        volatile String assignedWorkerId = null;
        StoredBatch(String batchId, String modelId, long createTs, TaskSeq tasks, int priority){
            this.batchId=batchId; this.modelId=modelId; this.createTs=createTs; this.tasks=tasks; this.priority=priority;
        }
    }

    /* -------------------- State -------------------- */
    private final Config cfg;
    private final OpenBatchEmitter openBatchEmitter;
    private final TaskListEmitter taskListEmitter;

    private final ConcurrentMap<Key, Window> windows = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, StoredBatch> pendingBatches = new ConcurrentHashMap<>();
    private final Deque<StoredBatch> servedRecently = new ConcurrentLinkedDeque<>();

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

    /** Fan-out into per-(model_id, priority) windows, priority parsed from request_id. */
    public void offer(InferenceRequest req){
        if (req == null || req.tasks == null || req.tasks.length() == 0) return;
        int pri = extractPriority(req.request_id); // 0/1/2 (from request_id)
        for (int i = 0; i < req.tasks.length(); i++) {
            SingleTask st = req.tasks.get_at(i);
            if (st == null) continue;
            offer(st, pri);
        }
    }

    /** Offer a single task with explicit priority. */
    public void offer(SingleTask st, int pri){
        if (st == null || st.payload == null) return;
        Key key = new Key(st.model_id, pri);
        Window w = windows.computeIfAbsent(key, k -> new Window(k.modelId, k.priority));
        if (w.q.size() >= cfg.perWindowCapacity) { w.q.pollFirst(); } // simple backpressure
        w.add(new PendingItem(st.request_id, st.task_id, st.client_id, st.payload));

        int maxBatch = effectiveMaxBatch(w.modelId, w.priority);
        if (w.size() >= maxBatch) cutAndAnnounce(w, maxBatch);
    }

    public void onGrant(Grant grant){
        if (grant == null) return;
        StoredBatch sb = pendingBatches.remove(grant.batch_id);
        if (sb == null) return;
        sb.assignedWorkerId = grant.winner_worker_id;
        emitTaskList(sb);
        servedRecently.addLast(sb);
        cleanupServed();
    }

    /* -------------------- Internal -------------------- */
    private void tick(){
        for (Window w : windows.values()){
            if (w.isEmpty()) continue;
            int  maxBatch = effectiveMaxBatch(w.modelId, w.priority);
            long maxWait  = effectiveMaxWaitMs(w.modelId, w.priority);
            if (w.size() >= maxBatch || w.ageMs() >= maxWait){
                cutAndAnnounce(w, maxBatch);
            }
        }
        cleanupServed();
    }

    private int effectiveMaxBatch(String modelId, int pri){
        Integer override = cfg.perModelMaxBatch.get(modelId);
        if (override != null && override > 0) return override;
        switch (pri) {
            case 0: return Math.max(1, cfg.defaultMaxGpuBatch); // fast small batch
            case 1: return 1;                                   // no batching
            case 2:
            default: return Math.max(1, cfg.defaultMaxCpuBatch);
        }
    }

    private long effectiveMaxWaitMs(String modelId, int pri){
        Long override = cfg.perModelMaxWaitMs.get(modelId);
        if (override != null && override >= 0) return override;
        switch (pri) {
            case 0: return Math.max(0L, cfg.waitFastMs);  // very short
            case 1: return Math.max(0L, cfg.waitNoneMs);  // no wait
            case 2:
            default: return Math.max(0L, cfg.defaultMaxWaitMs);
        }
    }

    private void cutAndAnnounce(Window w, int maxBatch){
        List<PendingItem> items = w.drainUpTo(maxBatch);
        if (items.isEmpty()) return;

        String batchId = newBatchId(w.priority); // encode priority into batch_id
        TaskSeq seq = new TaskSeq();
        seq.ensure_length(items.size(), items.size());
        for (int i=0;i<items.size();i++) seq.set_at(i, items.get(i).toTask());

        StoredBatch sb = new StoredBatch(batchId, w.modelId, System.currentTimeMillis(), seq, w.priority);
        pendingBatches.put(batchId, sb);

        OpenBatch ob = new OpenBatch();
        ob.batch_id = batchId;
        ob.model_id = w.modelId;
        ob.size = items.size();
        ob.create_ts_ms = (int) sb.createTs; // keep IDL compatibility

        System.out.println("[TaskClassifier] OPEN batch: batch_id=" + ob.batch_id +
                " model_id=" + ob.model_id + " size=" + ob.size + " pri=" + w.priority);
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

    private String newBatchId(int p){
        return "b-p" + p + "-" + System.currentTimeMillis() + "-" + batchSeq.incrementAndGet();
    }

    /** Parse priority from request_id, e.g. "... priority:0". Default to DEFAULT_PRIORITY. */
    public static int extractPriority(String requestId){
        if (requestId == null) return DEFAULT_PRIORITY;
        int i = requestId.indexOf("priority:");
        if (i < 0) return DEFAULT_PRIORITY;
        try {
            String s = requestId.substring(i + 9).trim();
            int p = Integer.parseInt(s);
            return (p==0 || p==1) ? p : 2;
        } catch (Exception ignore){
            return DEFAULT_PRIORITY;
        }
    }
}
