package worker;

import data_structure.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.time.Instant;
import java.util.UUID;

public class ModelRunner  {

    /** 跑一整个 TaskList（真·批处理，跑完即清理） */
    public static WorkerResult runBatchedTask(TaskList tasks) {
        WorkerResult wr = new WorkerResult();
        wr.batch_id = tasks.batch_id;
        wr.model_id = tasks.model_id;

        int n = (tasks == null || tasks.tasks == null) ? 0 : tasks.tasks.length();
        WorkerTaskResultSeq out = new WorkerTaskResultSeq();
        out.ensure_length(n, n);

        if (n == 0) {
            wr.results = out;
            return wr;
        }

        RunPaths rp = null;
        try {
            rp = makeBatchRunPaths(tasks.batch_id);

            // 1) 写 inputs/<task_id>.jpg
            for (int i = 0; i < n; i++) {
                Task t = (Task) tasks.tasks.get_at(i);
                if (t == null || t.payload == null || t.payload.length() == 0) continue;
                int inLen = t.payload.length();
                byte[] inBytes = new byte[inLen];
                t.payload.to_array(inBytes, inLen);
                atomicWrite(rp.inputs.resolve(t.task_id + ".jpg"), inBytes);
            }

            // 2) 批推理
            runModelBatch(rp.inputs.toString(), rp.outputs.toString());

            // 3) 读输出
            for (int i = 0; i < n; i++) {
                Task t = (Task) tasks.tasks.get_at(i);
                WorkerTaskResult r = new WorkerTaskResult();
                r.request_id = t.request_id;
                r.task_id = t.task_id;
                r.client_id = t.client_id;

                try {
                    Path outPath = rp.outputs.resolve(t.task_id + ".jpg");
                    if (Files.exists(outPath)) {
                        byte[] outBytes = Files.readAllBytes(outPath);
                        r.status = "OK";
                        r.output_blob = toBytes(outBytes);
                    } else {
                        r.status = "ERROR_NO_OUTPUT";
                        r.output_blob = emptyBytes();
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    r.status = "ERROR";
                    r.output_blob = emptyBytes();
                }
                out.set_at(i, r);
            }

            wr.results = out;
            return wr;

        } catch (IOException e) {
            for (int i = 0; i < n; i++) {
                Task t = (Task) tasks.tasks.get_at(i);
                out.set_at(i, runSingleTask(t));
            }
            wr.results = out;
            return wr;

        } finally {
            deleteRecursiveQuietly(rp == null ? null : rp.root);
        }
    }

    /** 单条任务兜底，跑完即清理 */
    static WorkerTaskResult runSingleTask(Task task) {
        WorkerTaskResult r = new WorkerTaskResult();
        r.request_id = task.request_id;
        r.task_id = task.task_id;
        r.client_id = task.client_id;

        RunPaths rp = null;
        try {
            if (task == null || task.payload == null || task.payload.length() == 0) {
                r.status = "ERROR_INVALID_INPUT";
                r.output_blob = emptyBytes();
                return r;
            }

            rp = makeSingleRunPaths(task.task_id, task.request_id);

            int inLen = task.payload.length();
            byte[] inBytes = new byte[inLen];
            task.payload.to_array(inBytes, inLen);
            Path inputPath = rp.inputs.resolve(task.task_id + ".jpg");
            atomicWrite(inputPath, inBytes);

            runModelSingle(inputPath.toString(), task.task_id, rp.outputs.toString());

            Path outPath = rp.outputs.resolve(task.task_id + ".jpg");
            byte[] outBytes = Files.readAllBytes(outPath);
            r.status = "OK";
            r.output_blob = toBytes(outBytes);
            return r;

        } catch (Throwable e) {
            e.printStackTrace();
            r.status = "ERROR";
            r.output_blob = emptyBytes();
            return r;

        } finally {
            deleteRecursiveQuietly(rp == null ? null : rp.root);
        }
    }

    // ========== 调 Python ==========

    static void runModelBatch(String inputDir, String outputDir) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    findPythonExecutable(),
                    findPythonScriptPath(),
                    "--path", inputDir,
                    "--out_dir", outputDir
            );
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line; while ((line = br.readLine()) != null) System.out.println("[PYTHON] " + line);
            }
            System.out.println("batch exitcode=" + process.waitFor());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void runModelSingle(String inputPath, String taskId, String outputDir) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    findPythonExecutable(),
                    findPythonScriptPath(),
                    "--path", inputPath,
                    "--task_id", taskId,
                    "--out_dir", outputDir
            );
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line; while ((line = br.readLine()) != null) System.out.println("[PYTHON] " + line);
            }
            System.out.println("single exitcode=" + process.waitFor());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // ========== 路径/目录工具 ==========

    static final class RunPaths {
        final String runId;
        final Path root;
        final Path inputs;
        final Path outputs;
        RunPaths(String runId, Path root, Path inputs, Path outputs) {
            this.runId = runId; this.root = root; this.inputs = inputs; this.outputs = outputs;
        }
    }

    private static RunPaths makeBatchRunPaths(String batchId) throws IOException {
        String safeBatch = safe(batchId, "batch");
        String runId = Instant.now().toEpochMilli() + "_" + UUID.randomUUID().toString().substring(0, 8);
        Path root = Paths.get("workdir", "batches", safeBatch, runId);
        Path inputs = root.resolve("inputs");
        Path outputs = root.resolve("outputs");
        Files.createDirectories(inputs);
        Files.createDirectories(outputs);
        return new RunPaths(runId, root, inputs, outputs);
    }

    private static RunPaths makeSingleRunPaths(String taskId, String requestId) throws IOException {
        String safeTask = safe(taskId, "task");
        String safeReq  = safe(requestId, "req");
        String runId = Instant.now().toEpochMilli() + "_" + UUID.randomUUID().toString().substring(0, 8);
        Path root = Paths.get("workdir", "singles", safeTask + "_" + safeReq + "_" + runId);
        Path inputs = root.resolve("inputs");
        Path outputs = root.resolve("outputs");
        Files.createDirectories(inputs);
        Files.createDirectories(outputs);
        return new RunPaths(runId, root, inputs, outputs);
    }

    private static void atomicWrite(Path target, byte[] bytes) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".part");
        Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteRecursiveQuietly(Path root) {
        if (root == null) return;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                if (!Files.exists(root)) return;
                Files.walk(root)
                        .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignore) {} });
                return;
            } catch (IOException e) {
                try { Thread.sleep(80L * (attempt + 1)); } catch (InterruptedException ignored) {}
            }
        }
    }

    // ========== 路径查找/Bytes 工具 ==========

    private static String safe(String s, String def) {
        return (s == null || s.isEmpty()) ? def : s.replaceAll("[^a-zA-Z0-9_\\-]", "_");
    }

    private static String findPythonExecutable() {
        try {
            Process p = new ProcessBuilder("python", "--version").start();
            if (p.waitFor() == 0) return "python";
        } catch (Exception ignored) {}
        try {
            Process p = new ProcessBuilder("python3", "--version").start();
            if (p.waitFor() == 0) return "python3";
        } catch (Exception ignored) {}
        return "C:/Users/HWH/AppData/Local/Programs/Python/Python39/python.exe";
    }

    private static String findPythonScriptPath() {
        Path p1 = Paths.get("src", "yolo_service", "pred.py");
        if (Files.exists(p1)) return p1.toString();
        return Paths.get(System.getProperty("user.dir"), "src", "yolo_service", "pred.py").toString();
    }

    private static Bytes emptyBytes() {
        Bytes b = new Bytes();
        b.from_array(new byte[0], 0);
        return b;
    }

    private static Bytes toBytes(byte[] arr) {
        Bytes b = new Bytes();
        b.from_array(arr, arr.length);
        return b;
    }
}
