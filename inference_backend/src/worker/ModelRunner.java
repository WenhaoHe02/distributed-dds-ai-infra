package worker;

import data_structure.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ModelRunner  {

    /** 跑一整个 TaskList（批） */
    public static WorkerResult runBatchedTask(TaskList tasks) {
        WorkerResult wr = new WorkerResult();
        wr.batch_id = tasks.batch_id;
        wr.model_id = tasks.model_id;

        int n = (tasks == null || tasks.tasks == null) ? 0 : tasks.tasks.length();
        WorkerTaskResultSeq out = new WorkerTaskResultSeq();
        out.ensure_length(n, n);

        for (int i = 0; i < n; i++) {
            Task t = (Task) tasks.tasks.get_at(i);
            WorkerTaskResult r = runSingleTask(t);
            out.set_at(i, r);
        }

        wr.results = out;
        return wr;
    }

    /** 单条任务：保持你原有的写文件 → 调 python → 读输出的逻辑 */
    static WorkerTaskResult runSingleTask(Task task) {
        WorkerTaskResult r = new WorkerTaskResult();
        r.request_id = task.request_id;
        r.task_id = task.task_id;
        r.client_id = task.client_id;

        try {
            if (task == null || task.payload == null || task.payload.length() == 0) {
                r.status = "ERROR_INVALID_INPUT";
                r.output_blob = emptyBytes();
                return r;
            }

            // 1) 把 payload 写到固定目录（保持原路径规则）
            int inLen = task.payload.length();
            byte[] inBytes = new byte[inLen];
            task.payload.to_array(inBytes, inLen);

            Path inputPath = Paths.get("workdir", task.task_id + ".jpg");
            Files.createDirectories(inputPath.getParent());
            Files.write(inputPath, inBytes);

            // 2) 调用模型脚本（保持你原来的绝对路径）
            String outputPathStr = runModel(inputPath.toString(), task.task_id);

            // 3) 读结果文件
            Path outPath = Paths.get(outputPathStr);
            byte[] outBytes = Files.readAllBytes(outPath);

            r.status = "OK";
            r.output_blob = toBytes(outBytes);

        } catch (Throwable e) {
            e.printStackTrace();
            r.status = "ERROR";
            r.output_blob = emptyBytes();
        }
        return r;
    }

    /** 保持你原来的 python 调用方式和路径 */
    static String runModel(String inputPath, String taskId) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    findPythonExecutable(),  // 使用动态查找的 Python 路径
                    findPythonScriptPath(),  // 使用动态查找的脚本路径
                    "--path", inputPath,
                    "--task_id", taskId
            );
            pb.redirectErrorStream(true);
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[PYTHON] " + line);
            }

            int exitCode = process.waitFor();
            System.out.println("exitcode=" + exitCode);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        // 保持原有输出路径规则
        return Paths.get(
                findPythonScriptDirectory(),
                taskId + ".jpg"
        ).toString();
    }

    /**
     * 查找本机 Python 可执行文件路径
     */
    private static String findPythonExecutable() {
        // 尝试使用 "python" 命令
        try {
            Process process = new ProcessBuilder("python", "--version").start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                return "python";
            }
        } catch (Exception e) {
            // 忽略异常，尝试其他方式
        }

        // 尝试使用 "python3" 命令
        try {
            Process process = new ProcessBuilder("python3", "--version").start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                return "python3";
            }
        } catch (Exception e) {
            // 忽略异常
        }

        // 如果上述方式都不行，返回默认路径（原来的路径）
        return "C:/Users/HWH/AppData/Local/Programs/Python/Python39/python.exe";
    }

    /**
     * 查找 Python 脚本路径
     */
    private static String findPythonScriptPath() {
        // 使用相对于类路径的路径
        Path scriptPath = Paths.get("src", "yolo_service", "pred.py");
        if (Files.exists(scriptPath)) {
            return scriptPath.toString();
        }

        // 如果上面的路径不存在，尝试使用绝对路径
        scriptPath = Paths.get(System.getProperty("user.dir"), "src", "yolo_service", "pred.py");
        return scriptPath.toString();
    }

    /**
     * 查找 Python 脚本所在目录
     */
    private static String findPythonScriptDirectory() {
        Path scriptDir = Paths.get("src", "yolo_service");
        if (Files.exists(scriptDir)) {
            return scriptDir.toString();
        }

        scriptDir = Paths.get(System.getProperty("user.dir"), "src", "yolo_service");
        return scriptDir.toString();
    }

    /* ===================== Bytes 辅助 ===================== */
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