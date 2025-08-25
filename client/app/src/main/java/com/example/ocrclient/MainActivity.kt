package com.example.ocrclient
import android.content.Intent
import android.graphics.BitmapFactory
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.widget.CheckBox
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import com.example.ocrclient.databinding.ActivityMainBinding
import com.example.ocrclient.net.RetrofitClient
import com.example.ocrclient.net.ServerResult
import com.example.ocrclient.util.ImageUtils
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import retrofit2.Call
import retrofit2.Callback
import retrofit2.Response
import java.io.File

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private var ocrUris: MutableList<Uri> = mutableListOf()
    private var detectUris: MutableList<Uri> = mutableListOf()

    // OCR图片选择器
    private val pickOcrImagesLauncher =
        registerForActivityResult(ActivityResultContracts.OpenMultipleDocuments()) { uris ->
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

    // 物体检测图片选择器
    private val pickDetectImagesLauncher =
        registerForActivityResult(ActivityResultContracts.OpenMultipleDocuments()) { uris ->
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
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // OCR按钮点击事件
        binding.btnPickOcr.setOnClickListener {
            pickOcrImagesLauncher.launch(arrayOf("image/*"))
        }

        // 物体检测按钮点击事件
        binding.btnPickDetect.setOnClickListener {
            pickDetectImagesLauncher.launch(arrayOf("image/*"))
        }

        // 上传所有图片按钮点击事件
        binding.btnUploadAll.setOnClickListener {
            if (ocrUris.isEmpty() && detectUris.isEmpty()) {
                Toast.makeText(this, "请先选择图片", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }
            uploadAllImages()
        }
    }

    private fun showOcrPreview() {
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

    private fun showDetectPreview() {
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

    private fun setLoading(loading: Boolean) {
        binding.progress.visibility = if (loading) View.VISIBLE else View.GONE
        binding.btnPickOcr.isEnabled = !loading
        binding.btnPickDetect.isEnabled = !loading
        binding.btnUploadAll.isEnabled = !loading
    }

    private fun uploadAllImages() {
        setLoading(true)
        binding.tvResult.text = "上传中..."

        Thread {
            try {
                val imageParts = mutableListOf<MultipartBody.Part>()
                val tasks = mutableListOf<String>()
                
                // 处理OCR图片
                ocrUris.forEach { uri ->
                    val file: File =
                        ImageUtils.compressUriToJpegCache(this, uri, 1080, 1080, 85)
                    
                    val imgBody = file.asRequestBody("image/jpeg".toMediaType())
                    val imgPart = MultipartBody.Part.createFormData("images", file.name, imgBody)
                    imageParts.add(imgPart)
                    tasks.add("ocr")
                }
                
                // 处理检测图片
                detectUris.forEach { uri ->
                    val file: File =
                        ImageUtils.compressUriToJpegCache(this, uri, 1080, 1080, 85)
                    
                    val imgBody = file.asRequestBody("image/jpeg".toMediaType())
                    val imgPart = MultipartBody.Part.createFormData("images", file.name, imgBody)
                    imageParts.add(imgPart)
                    tasks.add("detect")
                }

                // 创建任务列表请求体
                val tasksJson = tasks.joinToString(",", "[", "]") { "\"$it\"" }
                val tasksPart = tasksJson.toRequestBody("application/json".toMediaType())

                runOnUiThread {
                    RetrofitClient.api.recognizeMixed(tasksPart, imageParts)
                        .enqueue(object : Callback<ServerResult> {
                            override fun onResponse(
                                call: Call<ServerResult>,
                                response: Response<ServerResult>
                            ) {
                                setLoading(false)
                                if (response.isSuccessful && response.body() != null) {
                                    val r = response.body()!!
                                    binding.tvResult.text = when (r.task) {
                                        "mixed" -> {
                                            val sb = StringBuilder("混合任务结果：\n")
                                            r.objects?.forEach { o ->
                                                sb.append("- ${o.label} (${o.score})\n")
                                            }
                                            sb.toString()
                                        }
                                        else -> "未知任务：${r.task}\nmessage=${r.message}"
                                    }
                                } else {
                                    binding.tvResult.text = "响应失败 HTTP ${response.code()}"
                                }
                            }

                            override fun onFailure(call: Call<ServerResult>, t: Throwable) {
                                setLoading(false)
                                binding.tvResult.text = "请求失败：${t.message}"
                            }
                        })
                }
            } catch (e: Exception) {
                runOnUiThread {
                    setLoading(false)
                    binding.tvResult.text = "图片处理失败：${e.message}"
                }
            }
        }.start()
    }
}