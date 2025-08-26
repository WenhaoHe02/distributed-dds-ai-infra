package dispatcher;

import data_structure.*; // InferenceRequest, SingleTask, TaskList, SingleTaskSeq, KV/KVList/KVSeq
import java.util.*;
import java.util.concurrent.*;

/**
 * TaskClassifier (no checkpoint):
 * - offer(InferenceRequest): 将请求按涉及到的 model_id 投递到各自 ModelAgg
 * - ModelAgg: 取到一个请求后，把该请求中属于本 model 的所有任务一次性处理完
 *             （按 maxBatch/maxBytes 切成多个 TaskList 依次 emit），不与其他请求混批
 */
public class TaskClassifier {

    public interface Emitter {
        void emit(String modelId, TaskList tl);
    }

    public static final class Config {
        /** 每个 TaskList 最大样本数（不跨请求凑批）。先不打批就设 1；要小批设 4/8/16 */
        public int maxBatch = 4;
        /** 单批输入总字节上限；0 表示不限制（保护内存/带宽可启用） */
        public int maxBytes = 0;
        /** 可按模型覆盖批大小（优先于 maxBatch） */
        public int maxWaitMs = 0;
        public Map<String, Integer> perModelMaxBatch = new ConcurrentHashMap<>();
    }

    private final Config cfg;
    private final Emitter emitter;
    private final ConcurrentMap<String, ModelAgg> models = new ConcurrentHashMap<>();

    public TaskClassifier(Config cfg, Emitter emitter) {
        this.cfg = cfg;
        this.emitter = emitter;
    }

    /** 投喂整条请求：将该请求投递到它涉及到的每个 model 的队列（一次入队，不拷贝数据） */
    public void offer(InferenceRequest req) {
        if (req == null || req.input_blob == null || req.input_blob.length() == 0) return;

        // 找出该 request 涉及的模型集合
        Set<String> modelSet = new HashSet<>();
        for (int i = 0; i < req.input_blob.length(); i++) {
            SingleTask t = req.input_blob.get_at(i);
            if (t != null && t.model_id != null && !t.model_id.isEmpty()) {
                modelSet.add(t.model_id);
            }
        }
        if (modelSet.isEmpty()) return;

        for (String model : modelSet) {
            models.computeIfAbsent(model, m -> new ModelAgg(m, cfg, emitter))
                    .enqueue(req);
        }
    }

    /** 每个 model 一个聚合器：一次处理完一个请求（不与其他请求混批） */
    private static final class ModelAgg {
        private final String model;
        private final Config cfg;
        private final Emitter emitter;

        private final BlockingQueue<InferenceRequest> q = new LinkedBlockingQueue<>(10_000);
        private final Thread loopThread;

        ModelAgg(String model, Config cfg, Emitter emitter) {
            this.model = model;
            this.cfg = cfg;
            this.emitter = emitter;
            this.loopThread = new Thread(this::loop, "agg-" + model);
            this.loopThread.setDaemon(true);
            this.loopThread.start();
        }

        void enqueue(InferenceRequest req) {
            if (!q.offer(req)) {
                System.err.println("[TaskClassifier] queue full, drop request_id=" + req.request_id + " model=" + model);
            }
        }

        void loop() {
            for (;;) {
                try {
                    InferenceRequest req = q.take(); // 取一个请求，必须把它处理完（对该 model）
                    if (req == null || req.input_blob == null || req.input_blob.length() == 0) continue;

                    // 收集该请求中属于本 model 的所有任务
                    List<SingleTask> all = new ArrayList<>();
                    for (int i = 0; i < req.input_blob.length(); i++) {
                        SingleTask t = req.input_blob.get_at(i);
                        if (t != null && model.equals(t.model_id)) {
                            all.add(t);
                        }
                    }
                    if (all.isEmpty()) continue;

                    // 批量切分（不跨请求；按顺序连续发布）
                    int maxBatch = cfg.perModelMaxBatch.getOrDefault(model, cfg.maxBatch);
                    if (maxBatch < 1) maxBatch = 1;

                    int idx = 0;
                    while (idx < all.size()) {
                        int count = 0;
                        int bytes = 0;
                        // 计算本批能放多少（先按 maxBatch，再看 maxBytes）
                        while (idx + count < all.size() && count < maxBatch) {
                            SingleTask t = all.get(idx + count);
                            int l = (t.input_blob != null) ? t.input_blob.length() : 0;
                            if (cfg.maxBytes > 0 && bytes + l > cfg.maxBytes) {
                                break;
                            }
                            bytes += l;
                            count++;
                        }
                        if (count == 0) { // 单条就超 maxBytes（或为 0 上限），也要至少发 1 条避免卡住
                            count = 1;
                        }

                        emitSlice(all, idx, count);
                        idx += count;
                    }

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        private void emitSlice(List<SingleTask> all, int start, int count) {
            TaskList tl = new TaskList();
            tl.tasks = new SingleTaskSeq();
            tl.tasks.ensure_length(count, count);
            for (int i = 0; i < count; i++) {
                tl.tasks.set_at(i, all.get(start + i));
            }
            tl.task_num = count;
            tl.worker_id = "auto";
            tl.meta = null;
            emitter.emit(model, tl);
        }
    }
}
