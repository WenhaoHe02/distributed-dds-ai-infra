package com.example.ocrclient.util;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.util.Log;

/**
 * 图像转换工具类
 */
public class ImageConversionUtil {
    private static final String TAG = "ImageConversionUtil";

    /**
     * 将字节数组转换为Bitmap图像
     * @param byteArray 字节数组
     * @return Bitmap图像，如果转换失败则返回null
     */
    public static Bitmap byteArrayToBitmap(byte[] byteArray) {
        // 检查输入参数
        if (byteArray == null) {
            Log.w(TAG, "输入字节数组为null");
            return null;
        }
        
        if (byteArray.length == 0) {
            Log.w(TAG, "输入字节数组为空");
            return null;
        }
        
        try {
            // 尝试将字节数组解码为Bitmap
            Bitmap bitmap = BitmapFactory.decodeByteArray(byteArray, 0, byteArray.length);
            if (bitmap == null) {
                Log.w(TAG, "无法将字节数组解码为Bitmap，字节数组长度: " + byteArray.length);
                return null;
            }
            
            Log.d(TAG, "成功将字节数组转换为Bitmap，尺寸: " + bitmap.getWidth() + "x" + bitmap.getHeight());
            return bitmap;
        } catch (Exception e) {
            Log.e(TAG, "将字节数组转换为Bitmap时发生异常", e);
            return null;
        }
    }
}