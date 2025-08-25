package com.example.ocrclient.util

import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.net.Uri
import java.io.File
import java.io.FileOutputStream

object ImageUtils {
    fun compressUriToJpegCache(
        ctx: Context,
        uri: Uri,
        maxW: Int,
        maxH: Int,
        quality: Int
    ): File {
        val resolver = ctx.contentResolver

        // 先读取尺寸
        val opts = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        resolver.openInputStream(uri).use {
            BitmapFactory.decodeStream(it, null, opts)
        }

        val inSample = calcInSampleSize(opts.outWidth, opts.outHeight, maxW, maxH)
        val opts2 = BitmapFactory.Options().apply { inSampleSize = inSample }
        val bitmap = resolver.openInputStream(uri)?.use {
            BitmapFactory.decodeStream(it, null, opts2)
        } ?: throw Exception("无法解码图片")

        val out = File.createTempFile("upload_", ".jpg", ctx.cacheDir)
        FileOutputStream(out).use {
            bitmap.compress(Bitmap.CompressFormat.JPEG, quality, it)
        }
        bitmap.recycle()
        return out
    }

    private fun calcInSampleSize(w: Int, h: Int, reqW: Int, reqH: Int): Int {
        var inSample = 1
        if (h > reqH || w > reqW) {
            var halfH = h / 2
            var halfW = w / 2
            while ((halfH / inSample) >= reqH && (halfW / inSample) >= reqW) {
                inSample *= 2
            }
        }
        return inSample
    }
}
