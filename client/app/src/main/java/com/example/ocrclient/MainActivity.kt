package com.example.ocrclient

import android.content.Intent
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.net.Uri
import android.os.Bundle
import android.provider.Settings
import android.util.Log
import android.view.View
import android.widget.ImageView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import com.example.ocrclient.databinding.ActivityMainBinding
import com.example.ocrclient.util.ImageConversionUtil
import com.example.ocrclient.util.ImageUtils
import java.util.UUID
import java.util.Collections
import java.util.HashSet
import java.util.concurrent.ConcurrentHashMap

/**
 * MainActivity是应用的主界面Activity
 * 负责处理图片选择、预览和上传功能
 */
class MainActivity : AppCompatActivity() {
    companion object {
        private const val TAG = "MainActivity"
    }

    private lateinit var binding: ActivityMainBinding
    // 存储选择的OCR任务图片URI列表
    private var ocrUris: MutableList<Uri> = mutableListOf()
    // 存储选择的检测任务图片URI列表
    private var detectUris: MutableList<Uri> = mutableListOf()
    
    // DDS服务
    private lateinit var ddsSendService: DDSSendService
    private lateinit var dataSend: DataSend
    
    // 结果处理器
    private lateinit var resultHandler: ResultHandler
    
    // 存储已发送的请求ID和客户端ID的映射关系
    private val sentRequests: MutableMap<String, String> = ConcurrentHashMap()
    
    // 客户端ID，基于设备Android ID生成
    private val clientId: String by lazy {
        val androidId = Settings.Secure.getString(contentResolver, Settings.Secure.ANDROID_ID)
        // 如果无法获取Android ID，则使用UUID作为备用方案
        val id = androidId ?: UUID.randomUUID().toString().substring(0, 16)
        Log.d(TAG, "客户端ID: $id")
        id
    }

    // OCR图片选择器，支持多选
    private val pickOcrImagesLauncher =
        registerForActivityResult(ActivityResultContracts.OpenMultipleDocuments()) { uris ->
            Log.d(TAG, "选择了 ${uris.size} 个OCR图片")
            if (uris.isNotEmpty()) {
                uris.forEach { uri ->
                    contentResolver.takePersistableUriPermission(
                        uri, Intent.FLAG_GRANT_READ_URI_PERMISSION
                    )
                    ocrUris.add(uri)
                }
                showOcrPreview()
            }
        }

    // 物体检测图片选择器，支持多选
    private val pickDetectImagesLauncher =
        registerForActivityResult(ActivityResultContracts.OpenMultipleDocuments()) { uris ->
            Log.d(TAG, "选择了 ${uris.size} 个检测图片")
            if (uris.isNotEmpty()) {
                uris.forEach { uri ->
                    contentResolver.takePersistableUriPermission(
                        uri, Intent.FLAG_GRANT_READ_URI_PERMISSION
                    )
                    detectUris.add(uri)
                }
                showDetectPreview()
            }
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        //创建消息接收
        val a = DDSReceiveService()
        a.setMainActivity(this) // 传递MainActivity引用给DDSReceiveService
        a.work()
        //logcat日志
        Log.d("MainActivity", "消息接收开始")


        super.onCreate(savedInstanceState)
        Log.d(TAG, "MainActivity onCreate")
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)
        
        // 初始化结果处理器
        resultHandler = ResultHandler(this, binding.imageContainerResult)
        
        // 初始化所有服务
        initializeServices()

        // OCR按钮点击事件
        binding.btnPickOcr.setOnClickListener {
            Log.d(TAG, "点击OCR图片选择按钮")
            pickOcrImagesLauncher.launch(arrayOf("image/*"))
        }

        // 物体检测按钮点击事件
        binding.btnPickDetect.setOnClickListener {
            Log.d(TAG, "点击检测图片选择按钮")
            pickDetectImagesLauncher.launch(arrayOf("image/*"))
        }

        // 上传所有图片按钮点击事件
        binding.btnUploadAll.setOnClickListener {
            Log.d(TAG, "点击上传所有图片按钮")
            if (ocrUris.isEmpty() && detectUris.isEmpty()) {
                Log.w(TAG, "没有选择任何图片")
                Toast.makeText(this, "请先选择图片", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }
            uploadAllImages()
        }
    }

    /**
     * 初始化所有服务
     */
    private fun initializeServices() {
        Log.d(TAG, "开始初始化服务")
        // 初始化DDS服务
        ddsSendService = DDSSendService()
        val ddsInitialized = ddsSendService.initializeDDS()
        if (!ddsInitialized) {
            Log.e(TAG, "DDS初始化失败")
            Toast.makeText(this, "DDS初始化失败", Toast.LENGTH_LONG).show()
        } else {
            Log.d(TAG, "DDS初始化成功")
        }
        
        // 初始化数据发送服务
        dataSend = DataSend(this)
        dataSend.initialize(ddsSendService)
        Log.d(TAG, "服务初始化完成")
    }

    override fun onDestroy() {
        super.onDestroy()
        Log.d(TAG, "MainActivity onDestroy")
        ddsSendService.releaseDDS()
    }

    /**
     * 显示OCR图片预览
     */
    private fun showOcrPreview() {
        Log.d(TAG, "显示OCR图片预览，数量: ${ocrUris.size}")
        if (ocrUris.isNotEmpty()) {
            binding.tvOcrInfo.text = "已选择 ${ocrUris.size} 张OCR图片"
            
            // 清空之前的预览
            binding.imageContainerOcr.removeAllViews()
            
            // 为每张图片创建预览
            ocrUris.forEach { uri ->
                val imageView = ImageView(this).apply {
                    scaleType = ImageView.ScaleType.CENTER_CROP
                    layoutParams = android.view.ViewGroup.LayoutParams(
                        200, // 宽度200dp
                        android.view.ViewGroup.LayoutParams.MATCH_PARENT
                    )
                }
                
                // 从URI加载图片
                contentResolver.openInputStream(uri).use { inputStream ->
                    val bmp = BitmapFactory.decodeStream(inputStream)
                    imageView.setImageBitmap(bmp)
                }
                
                // 添加到容器中
                binding.imageContainerOcr.addView(imageView)
            }
        } else {
            binding.tvOcrInfo.text = "尚未选择OCR图片"
            binding.imageContainerOcr.removeAllViews()
        }
    }

    /**
     * 显示检测图片预览
     */
    private fun showDetectPreview() {
        Log.d(TAG, "显示检测图片预览，数量: ${detectUris.size}")
        if (detectUris.isNotEmpty()) {
            binding.tvDetectInfo.text = "已选择 ${detectUris.size} 张检测图片"
            
            // 清空之前的预览
            binding.imageContainerDetect.removeAllViews()
            
            // 为每张图片创建预览
            detectUris.forEach { uri ->
                val imageView = ImageView(this).apply {
                    scaleType = ImageView.ScaleType.CENTER_CROP
                    layoutParams = android.view.ViewGroup.LayoutParams(
                        200, // 宽度200dp
                        android.view.ViewGroup.LayoutParams.MATCH_PARENT
                    )
                }
                
                // 从URI加载图片
                contentResolver.openInputStream(uri).use { inputStream ->
                    val bmp = BitmapFactory.decodeStream(inputStream)
                    imageView.setImageBitmap(bmp)
                }
                
                // 添加到容器中
                binding.imageContainerDetect.addView(imageView)
            }
        } else {
            binding.tvDetectInfo.text = "尚未选择检测图片"
            binding.imageContainerDetect.removeAllViews()
        }
    }

    /**
     * 设置加载状态
     * @param loading 是否正在加载
     */
    private fun setLoading(loading: Boolean) {
        Log.d(TAG, "设置加载状态: $loading")
        binding.progress.visibility = if (loading) View.VISIBLE else View.GONE
        binding.btnPickOcr.isEnabled = !loading
        binding.btnPickDetect.isEnabled = !loading
        binding.btnUploadAll.isEnabled = !loading
    }
    
    /**
     * 将图片URI转换为字节数组
     * @param uri 图片URI
     * @return 图片字节数组
     */
    private fun uriToByteArray(uri: Uri): ByteArray {
        contentResolver.openInputStream(uri)?.use { inputStream ->
            val file = ImageUtils.compressUriToJpegCache(this, uri, 1080, 1080, 85)
            return file.readBytes()
        }
        return ByteArray(0)
    }

    /**
     * 上传所有图片
     */
    private fun uploadAllImages() {
        Log.d(TAG, "开始上传所有图片")
        setLoading(true)
        binding.tvResult.text = "上传中..."

        Thread {
            try {
                Log.d(TAG, "在后台线程中发送图片")
                // 使用数据发送服务发送所有图片
                val request = dataSend.createInferenceRequest(ocrUris, detectUris)
                // 保存请求ID和客户端ID到映射中
                sentRequests[request.request_id] = clientId
                Log.d(TAG, "已添加请求ID和客户端ID到映射: ${request.request_id} -> $clientId")
                val success = ddsSendService.sendInferenceRequest(request)

                runOnUiThread {
                    setLoading(false)
                    if (success) {
                        binding.tvResult.text = "已通过DDS发送请求"
                        Log.d(TAG, "图片发送成功")
                        Toast.makeText(this, "请求发送成功", Toast.LENGTH_SHORT).show()
                    } else {
                        binding.tvResult.text = "发送请求失败"
                        Log.e(TAG, "图片发送失败")
                        Toast.makeText(this, "请求发送失败，请查看日志了解详情", Toast.LENGTH_LONG).show()
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "上传图片时发生异常", e)
                runOnUiThread {
                    setLoading(false)
                    binding.tvResult.text = "发送失败：${e.message}"
                    Toast.makeText(this, "发送失败：${e.message}", Toast.LENGTH_LONG).show()
                }
            }
        }.start()
    }
    
    /**
     * 处理接收到的响应结果
     */
    fun handleReceivedResult(resultInfo: String) {
        runOnUiThread {
            // 在结果文本框中追加新收到的结果
            val currentText = binding.tvResult.text.toString()
            binding.tvResult.text = if (currentText.isNotEmpty()) {
                "$currentText\n$resultInfo"
            } else {
                resultInfo
            }
            Log.d(TAG, "已更新结果显示: $resultInfo")
        }
    }
    
    /**
     * 处理AggregatedResult数据
     */
    fun handleAggregatedResult(result: com.example.ocrclient.ai.AggregatedResult) {
        val resultInfo = resultHandler.handleAggregatedResult(result)
        handleReceivedResult(resultInfo)
    }
    
    /**
     * 显示结果图像（已废弃，保留以兼容旧接口）
     */
    fun displayResultImages(imageBytesList: List<ByteArray>) {
        // 此方法已废弃，功能已整合到ResultHandler中
    }
    
    /**
     * 检查请求ID是否在已发送列表中，并且客户端ID是否匹配
     */
    fun isRequestValid(requestId: String, receivedClientId: String): Boolean {
        val isValid = sentRequests.containsKey(requestId) && sentRequests[requestId] == receivedClientId
        // 如果验证通过，从sentRequests中移除该请求，避免内存泄漏
        if (isValid) {
            sentRequests.remove(requestId)
        }
        return isValid
    }
}