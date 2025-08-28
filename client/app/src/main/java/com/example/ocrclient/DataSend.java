package com.example.ocrclient;

import android.content.Context;
import android.net.Uri;
import android.provider.Settings;
import android.util.Log;

import com.example.ocrclient.data_structure.*;
import com.example.ocrclient.data_structure.InferenceRequest;
import com.example.ocrclient.data_structure.SingleTask;
import com.example.ocrclient.data_structure.SingleTaskSeq;
import com.example.ocrclient.internal.RequestState;
import com.example.ocrclient.util.ImageUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class DataSend {
    private static final String TAG = "DataSend";
    private final Context context;
    private final String clientId;
    private DDSSendService ddsService;
    private static final String MODEL_ID_1 = "model_0";
    private static final String MODEL_ID_2 = "model_0";

    public DataSend(Context context) {
        this.context = context;
        // 获取Android ID
        String androidId = Settings.Secure.getString(context.getContentResolver(), Settings.Secure.ANDROID_ID);
        // 如果无法获取Android ID，则使用UUID作为备用方案
        this.clientId = (androidId != null) ? androidId : UUID.randomUUID().toString().substring(0, 16);
        Log.d(TAG, "DataSend初始化，客户端ID: " + this.clientId);
    }

    public void initialize(DDSSendService ddsService) {
        this.ddsService = ddsService;
        Log.d(TAG, "DataSend服务已初始化");
    }

    /**
     * 创建InferenceRequest对象，用于打包发送到服务端的数据
     *
     * @param ocrUris    OCR任务图片URI列表
     * @param detectUris 检测任务图片URI列表
     * @return InferenceRequest对象
     */
    public InferenceRequest createInferenceRequest(List<Uri> ocrUris, List<Uri> detectUris) {
        Log.d(TAG, "开始创建InferenceRequest对象");
        Log.d(TAG, "OCR任务数量: " + ocrUris.size() + ", 检测任务数量: " + detectUris.size());

        InferenceRequest request = new InferenceRequest();
        // 获取Android ID
        String androidId = Settings.Secure.getString(context.getContentResolver(), Settings.Secure.ANDROID_ID);
        // 结合UUID和Android ID生成唯一的请求ID（会有一点长）
        request.request_id = UUID.randomUUID() + "_" + androidId;

        Log.d(TAG, "请求ID: " + request.request_id);

        // 创建任务序列
        SingleTaskSeq taskSeq = new SingleTaskSeq();
        taskSeq.ensure_length(ocrUris.size() + detectUris.size(), ocrUris.size() + detectUris.size());

        Log.d(TAG, "任务序列长度已设置为: " + (ocrUris.size() + detectUris.size()));

        // 处理OCR图片任务
        for (int i = 0; i < ocrUris.size(); i++) {
            SingleTask task = new SingleTask();
            task.request_id = request.request_id;
            task.model_id = MODEL_ID_1;
            task.client_id = clientId;
            task.task_id = String.valueOf(i + 1); // 从1开始自增

            // 将图片转换为字节数组并存储到input_blob中
            byte[] imageBytes = uriToByteArray(ocrUris.get(i));
            task.payload.from_array(imageBytes, imageBytes.length);

            Log.d(TAG, "OCR任务 " + i + " 创建完成，任务ID: " + task.task_id + "，图片大小: " + imageBytes.length + " 字节");

            // 添加到任务序列中
            taskSeq.set_at(i, task);
        }

        // 处理检测图片任务
        for (int i = 0; i < detectUris.size(); i++) {
            SingleTask task = new SingleTask();
            task.request_id = request.request_id;
            task.model_id = MODEL_ID_2;
            task.client_id = clientId;
            task.task_id = String.valueOf(ocrUris.size() + i + 1); // 继续从OCR任务之后自增

            // 将图片转换为字节数组并存储到input_blob中
            byte[] imageBytes = uriToByteArray(detectUris.get(i));
            task.payload.from_array(imageBytes, imageBytes.length);

            Log.d(TAG, "检测任务 " + i + " 创建完成，任务ID: " + task.task_id + "，图片大小: " + imageBytes.length + " 字节");

            // 添加到任务序列中
            taskSeq.set_at(ocrUris.size() + i, task);
        }

        request.tasks = taskSeq;
        Log.d(TAG, "InferenceRequest对象创建完成");
        return request;
    }

    /**
     * 将图片URI转换为字节数组
     *
     * @param uri 图片URI
     * @return 图片字节数组
     */
    private byte[] uriToByteArray(Uri uri) {
        try {
            Log.d(TAG, "开始转换图片URI为字节数组: " + uri.toString());
            // 修复：正确调用Kotlin对象的方法并使用返回的文件
            File file = ImageUtils.INSTANCE.compressUriToJpegCache(context, uri, 1080, 1080, 85);
            Log.d(TAG, "图片压缩完成，文件路径: " + file.getAbsolutePath() + "，文件大小: " + file.length() + " 字节");
            // 修复：使用FileInputStream读取文件内容
            byte[] bytes = readFileBytes(file);
            Log.d(TAG, "文件读取完成，字节数组大小: " + bytes.length + " 字节");
            return bytes;
        } catch (Exception e) {
            Log.e(TAG, "转换图片URI为字节数组时发生异常", e);
            e.printStackTrace();
            return new byte[0];
        }
    }

    /**
     * 读取文件内容为字节数组
     *
     * @param file 文件
     * @return 文件内容字节数组
     * @throws IOException IO异常
     */
    private byte[] readFileBytes(File file) throws IOException {
        Log.d(TAG, "开始读取文件内容: " + file.getAbsolutePath());
        FileInputStream fis = new FileInputStream(file);
        byte[] bytes = new byte[(int) file.length()];
        fis.read(bytes);
        fis.close();
        Log.d(TAG, "文件读取完成，大小: " + bytes.length + " 字节");
        return bytes;
    }

    /**
     * 发送所有图片请求
     */
    public boolean sendAllImages(List<Uri> ocrUris, List<Uri> detectUris) {
        try {
            Log.d(TAG, "开始发送所有图片请求");
            Log.d(TAG, "OCR图片数量: " + ocrUris.size() + "，检测图片数量: " + detectUris.size());

            // 创建InferenceRequest对象
            InferenceRequest inferenceRequest = createInferenceRequest(ocrUris, detectUris);

            // 将请求ID和客户端ID添加到ResultDataManager中，以便验证返回的数据
            ResultDataManager resultDataManager = ResultDataManager.getInstance();
            resultDataManager.addSentRequest(inferenceRequest.request_id, clientId);

            // 创建并注册RequestState对象
            RequestState requestState = new RequestState(inferenceRequest.request_id);
            // 设置预期任务数
            requestState.setExpectedTaskCount(ocrUris.size() + detectUris.size());
            // 将RequestState添加到ResultDataManager中
            resultDataManager.registerRequestState(requestState);

            Log.d(TAG, "已注册RequestState，预期任务数: " + (ocrUris.size() + detectUris.size()));

            Log.d(TAG, "InferenceRequest创建完成");

            // 使用DDS服务发送InferenceRequest对象
            boolean result = ddsService.sendInferenceRequest(inferenceRequest);
            Log.d(TAG, "DDS发送结果: " + (result ? "成功" : "失败"));
            return result;
        } catch (Exception e) {
            Log.e(TAG, "发送所有图片请求时发生异常", e);
            e.printStackTrace();
            return false;
        }
    }
}