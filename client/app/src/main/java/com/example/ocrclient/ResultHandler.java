package com.example.ocrclient;

import android.app.Activity;
import android.graphics.Color;
import android.util.Log;
import android.util.TypedValue;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.Toast;

import com.example.ocrclient.ai.AggregatedResult;
import com.example.ocrclient.ai.SingleResult;
import com.example.ocrclient.util.ImageConversionUtil;
import com.example.ocrclient.util.ResultSortUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 结果处理器
 * 负责处理从服务端返回的AggregatedResult数据，包括结果排序、图像提取和显示
 */
public class ResultHandler {
    private static final String TAG = "ResultHandler";
    private final Activity activity;
    private final LinearLayout imageContainer;

    /**
     * 构造函数
     * @param activity 关联的Activity
     * @param imageContainer 用于显示图像的容器
     */
    public ResultHandler(Activity activity, LinearLayout imageContainer) {
        this.activity = activity;
        this.imageContainer = imageContainer;
    }

    /**
     * 处理AggregatedResult数据
     * @param result AggregatedResult数据
     * @return 格式化的结果信息字符串
     */
    public String handleAggregatedResult(AggregatedResult result) {
        // 构造结果信息字符串
        StringBuilder resultInfo = new StringBuilder();
        resultInfo.append("收到响应 - 请求ID: ").append(result.request_id)
                .append(", 客户端ID: ").append(result.client_id)
                .append(", 状态: ").append(result.status);

        if (result.error_message != null && !result.error_message.isEmpty()) {
            resultInfo.append(", 错误: ").append(result.error_message);
        }

        // 创建用于存储图像字节数据的列表
        List<byte[]> imageBytesList = new ArrayList<>();

        // 如果有结果数据，则添加结果信息
        if (result.results != null && result.results.length() > 0) {
            // 使用排序工具对结果按照task_id进行排序
            List<SingleResult> sortedResults = ResultSortUtil.sortResultsByTaskId(result.results);

            resultInfo.append(", 结果数量: ").append(sortedResults.size());
            for (int j = 0; j < sortedResults.size(); j++) {
                // 按照排序后的顺序处理每个结果的详细信息
                SingleResult singleResult = sortedResults.get(j);
                resultInfo.append("\n  结果[").append(j).append("] taskId: ")
                        .append(singleResult.task.task_id)
                        .append(", status: ").append(singleResult.status);

                // 检查是否有输出数据
                if (singleResult.output_blob != null && singleResult.output_blob.length() > 0) {
                    // 提取图像字节数据
                    try {
                        byte[] imageData = new byte[singleResult.output_blob.length()];
                        for (int k = 0; k < imageData.length; k++) {
                            imageData[k] = singleResult.output_blob.get_at(k);
                        }

                        if (imageData.length > 0) {
                            imageBytesList.add(imageData);
                            Log.d(TAG, "提取到第 " + j + " 个结果的图像数据，大小: " + imageData.length + " 字节");
                        } else {
                            Log.w(TAG, "第 " + j + " 个结果的图像数据为空或长度为0");
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "提取第 " + j + " 个结果的图像数据时发生异常", e);
                    }
                } else {
                    Log.w(TAG, "第 " + j + " 个结果没有输出数据");
                }
            }
        }

        // 显示结果图像
        displayResultImages(imageBytesList);
        
        return resultInfo.toString();
    }

    /**
     * 显示结果图像
     * @param imageBytesList 图像字节数据列表
     */
    public void displayResultImages(List<byte[]> imageBytesList) {
        activity.runOnUiThread(new Runnable() {
            @Override
            public void run() {
                try {
                    // 清空之前的图像
                    imageContainer.removeAllViews();

                    if (imageBytesList == null || imageBytesList.isEmpty()) {
                        Log.d(TAG, "没有结果图像需要显示");
                        addPlaceholderImage();
                        return;
                    }

                    Log.d(TAG, "开始显示 " + imageBytesList.size() + " 个结果图像");

                    // 为每个字节数组创建图像视图
                    for (int i = 0; i < imageBytesList.size(); i++) {
                        byte[] byteArray = imageBytesList.get(i);

                        // 检查字节数组是否为空
                        if (byteArray == null) {
                            Log.w(TAG, "第 " + i + " 个结果图像字节数组为null");
                            addPlaceholderImage();
                            continue;
                        }

                        if (byteArray.length == 0) {
                            Log.w(TAG, "第 " + i + " 个结果图像字节数组为空");
                            addPlaceholderImage();
                            continue;
                        }

                        // 将字节数组转换为Bitmap
                        android.graphics.Bitmap bitmap = ImageConversionUtil.byteArrayToBitmap(byteArray);
                        if (bitmap == null) {
                            Log.w(TAG, "第 " + i + " 个结果图像转换失败");
                            addPlaceholderImage();
                            continue;
                        }

                        // 创建ImageView并设置Bitmap
                        ImageView imageView = new ImageView(activity);
                        imageView.setImageBitmap(bitmap);
                        imageView.setScaleType(ImageView.ScaleType.CENTER_CROP);
                        imageView.setLayoutParams(new ViewGroup.LayoutParams(200, 200));

                        // 添加到结果图像容器中
                        imageContainer.addView(imageView);
                        Log.d(TAG, "成功显示第 " + i + " 个结果图像，尺寸: " + bitmap.getWidth() + "x" + bitmap.getHeight());
                    }

                    Log.d(TAG, "结果图像显示完成");
                } catch (Exception e) {
                    Log.e(TAG, "显示结果图像时发生异常", e);
                    activity.runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            Toast.makeText(activity, "显示结果图像时发生异常: " + e.getMessage(), Toast.LENGTH_LONG).show();
                        }
                    });
                }
            }
        });
    }

    /**
     * 添加占位图像
     */
    private void addPlaceholderImage() {
        ImageView placeholder = new ImageView(activity);

        int size = (int) TypedValue.applyDimension(
                TypedValue.COMPLEX_UNIT_DIP, 200, activity.getResources().getDisplayMetrics()
        );
        ViewGroup.LayoutParams params = new ViewGroup.LayoutParams(size, size);
        placeholder.setLayoutParams(params);

        // 设置纯蓝色背景
        placeholder.setBackgroundColor(Color.BLACK);

        // 添加到容器中
        imageContainer.addView(placeholder);

        Log.d(TAG, "占位符已添加，当前容器子view数: " + imageContainer.getChildCount());
    }
}