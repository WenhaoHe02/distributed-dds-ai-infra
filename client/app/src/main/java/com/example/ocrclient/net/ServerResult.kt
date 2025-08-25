package com.example.ocrclient.net

data class ServerResult(
    val task: String?,
    val text: String?,
    val objects: List<DetectedObject>?,
    val message: String?
)

data class DetectedObject(
    val label: String?,
    val score: Double?,
    val box: List<Int>?,
    val taskType: String? // 添加任务类型字段，用于区分OCR和检测结果
)