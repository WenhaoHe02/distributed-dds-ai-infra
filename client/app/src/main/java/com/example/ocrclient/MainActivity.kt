package com.example.ocrclient

import android.content.Intent
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
import com.example.ocrclient.internal.RequestState
import com.example.ocrclient.util.ImageUtils
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * MainActivity是应用的主界面Activity
 * 负责处理图片选择、预览和上传功能
 */
class MainActivity : AppCompatActivity() {
    companion object {
        private const val TAG = "MainActivity"
        const val EXTRA_OCR_URIS = "ocr_uris"
        const val EXTRA_DETECT_URIS = "detect_uris"
        const val EXTRA_REQUEST_ID = "request_id"
    }

    private lateinit var binding: ActivityMainBinding
    // 存储选择的OCR任务图片URI列表
    private var ocrUris: MutableList<Uri> = mutableListOf()
    // 存储选择的检测任务图片URI列表
    private var detectUris: MutableList<Uri> = mutableListOf()
    
    // DDS服务
    private lateinit var ddsSendService: DDSSendService
    private lateinit var dataSend: DataSend

    private lateinit var ddsReceiveService: DDSReceiveService
    
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
        super.onCreate(savedInstanceState)
        Log.d(TAG, "MainActivity onCreate")
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)
        
        // 初始化所有服务
        initializeServices()

        //创建消息接收
        ddsReceiveService = DDSReceiveService()
        // 不再需要设置MainActivity引用
        ddsReceiveService.work()
        Log.d("MainActivity", "消息接收开始")

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

        Thread {
            try {
                Log.d(TAG, "在后台线程中发送图片")
                // 使用数据发送服务发送所有图片
                val request = dataSend.createInferenceRequest(ocrUris, detectUris)
//                // 保存请求ID和客户端ID到映射中
//                sentRequests[request.request_id] = clientId
                // 同时添加到ResultDataManager中用于验证
                ResultDataManager.getInstance().addSentRequest(request.request_id, clientId)

                // 创建并注册RequestState对象
                val requestState = RequestState(request.request_id)

                // 设置预期任务数
                requestState.setExpectedTaskCount((ocrUris.size + detectUris.size).toLong())

                // 将RequestState添加到ResultDataManager中
                ResultDataManager.getInstance().registerRequestState(requestState)

                Log.d(TAG, "已添加请求ID和客户端ID到映射: ${request.request_id} -> $clientId")
                val success = ddsSendService.sendInferenceRequest(request)

                runOnUiThread {
                    setLoading(false)
                    if (success) {
                        // 跳转到结果页面
                        val intent = Intent(this, ResultActivity::class.java).apply {
                            // 传递图片URI列表
                            putStringArrayListExtra(EXTRA_OCR_URIS, ArrayList(ocrUris.map { it.toString() }))
                            putStringArrayListExtra(EXTRA_DETECT_URIS, ArrayList(detectUris.map { it.toString() }))
                            putExtra(EXTRA_REQUEST_ID, request.request_id)
                        }
                        startActivity(intent)
                        Log.d(TAG, "图片发送成功，跳转到结果页面")
                    } else {
                        Log.e(TAG, "图片发送失败")
                        Toast.makeText(this, "请求发送失败，请查看日志了解详情", Toast.LENGTH_LONG).show()
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "上传图片时发生异常", e)
                runOnUiThread {
                    setLoading(false)
                    Toast.makeText(this, "发送失败：${e.message}", Toast.LENGTH_LONG).show()
                }
            }
        }.start()
    }
}