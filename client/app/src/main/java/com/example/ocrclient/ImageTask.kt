package com.example.ocrclient

import android.net.Uri

data class ImageTask(
    val uri: Uri,
    val taskType: TaskType
)

enum class TaskType {
    OCR,
    DETECT
}