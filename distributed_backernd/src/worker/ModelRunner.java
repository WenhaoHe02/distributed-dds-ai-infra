package worker;

import data_structure.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ModelRunner {
    static SingleResult runSingleTask(SingleTask task) {
        SingleResult r = new SingleResult();
        r.task = task; // 保留原始任务信息
        long t0 = System.nanoTime();
        try {
            if (task == null || task.input_blob == null || task.input_blob.length() == 0) {
                r.status = "ERROR_INVALID_INPUT";
                r.output_type = "NONE";
                r.output_blob = new Bytes();
                r.output_blob.from_array(new byte[0], 0);
                r.latency_ms = (System.nanoTime() - t0) / 1_000_000L;
                return r;
            }

            // 1) 把 input_blob 写到固定目录，文件名可用 task_id
            int inLen = task.input_blob.length();
            byte[] inBytes = new byte[inLen];
            task.input_blob.to_array(inBytes, inLen);

            Path inputPath = Paths.get("workdir", task.task_id + ".jpg");
            Files.createDirectories(inputPath.getParent());
            Files.write(inputPath, inBytes);

            // 2) 调模型，传入输入图片路径
            String outputPathStr = runModel(inputPath.toString(), task.task_id);

            // 3) 读结果图片，放进 SingleResult
            Path outPath = Paths.get(outputPathStr);
            byte[] outBytes = Files.readAllBytes(outPath);

            r.status = "OK";
            r.output_type = "IMAGE_JPG";
            r.output_blob = new Bytes();
            r.output_blob.from_array(outBytes, outBytes.length);

        } catch (Throwable e) {
            e.printStackTrace();
            r.status = "ERROR";
            r.output_type = "NONE";
            r.output_blob = new Bytes();
            r.output_blob.from_array(new byte[0], 0);
        } finally {
            r.latency_ms = (System.nanoTime() - t0) / 1_000_000L;
        }
        return r;
    }

    static String runModel(String inputDir, String taskId)
    {
        try
        {
            // 构建命令及参数（不要手动拼接字符串）
            ProcessBuilder pb = new ProcessBuilder(
                    "python",                  // Python 解释器命令（Linux 下可能是 python3）
                    "../yolo_service/pred.py",             // Python 脚本
                    "--path ../test/img1.jpg",
                    "--task_id" +" " + taskId

            );

            // 合并标准错误流
            pb.redirectErrorStream(true);

            // 启动进程
            Process process = pb.start();

            // 等待执行完成
            int exitCode = process.waitFor();

        }
        catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
        }
        return "../yolo_servic/" + taskId + ".jpg";
    }

    static WorkerResult runBatchedTask(TaskList tasks) {
        return new WorkerResult();
    }
}
