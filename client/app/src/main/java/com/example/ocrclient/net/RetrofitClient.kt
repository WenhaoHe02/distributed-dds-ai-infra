package com.example.ocrclient.net

import android.util.Log
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory

object RetrofitClient {
    // 模拟器访问本机用 http://10.0.2.2:8080/
    private const val BASE_URL = "http://10.0.2.2:8080/"

    val api: ApiService by lazy {
        val log = HttpLoggingInterceptor { msg -> Log.d("HTTP", msg) }
        log.level = HttpLoggingInterceptor.Level.BODY

        val ok = OkHttpClient.Builder()
            .addInterceptor(log)
            .build()

        Retrofit.Builder()
            .baseUrl(BASE_URL)
            .client(ok)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(ApiService::class.java)
    }
}