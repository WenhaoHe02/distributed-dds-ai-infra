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
        }
        return r;
    }

    static String runModel(String inputPath, String taskId) {
        try {
            // 用正确的参数格式：用两段式 "--param", "value"
            ProcessBuilder pb = new ProcessBuilder(
                    "C:/Users/HWH/AppData/Local/Programs/Python/Python39/python.exe",  // ✅ 用绝对路径！
                    "E:/distributed-dds-ai-serving-system/distributed_backernd/src/yolo_service/pred.py",
                    "--path", inputPath,
                    "--task_id", taskId
            );


            pb.redirectErrorStream(true);
            Process process = pb.start();

            // 打印 Python 输出
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

        // 正确拼接输出路径
        return Paths.get(
                "E:/distributed-dds-ai-serving-system/distributed_backernd/src/yolo_service",
                taskId + ".jpg"
        ).toString();
    }


    static WorkerResult runBatchedTask(TaskList tasks) {
        return new WorkerResult();
    }
}
