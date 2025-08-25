package com.example.ocrclient.net

import okhttp3.MultipartBody
import okhttp3.RequestBody
import retrofit2.Call
import retrofit2.http.Multipart
import retrofit2.http.POST
import retrofit2.http.Part

interface ApiService {

    @Multipart
    @POST("/api/recognize")
    fun recognizeSingle(
        @Part("task") task: RequestBody,
        @Part image: MultipartBody.Part
    ): Call<ServerResult>
    
    @Multipart
    @POST("/api/recognize")
    fun recognizeMultiple(
        @Part("task") task: RequestBody,
        @Part images: List<MultipartBody.Part>
    ): Call<ServerResult>
    
    @Multipart
    @POST("/api/recognize")
    fun recognizeMixed(
        @Part("tasks") tasks: RequestBody,
        @Part images: List<MultipartBody.Part>
    ): Call<ServerResult>
}