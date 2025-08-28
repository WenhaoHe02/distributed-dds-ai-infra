package com.example.ocrclient;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.FrameLayout;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.cardview.widget.CardView;

import com.example.ocrclient.data_structure.ResultItem;
import com.example.ocrclient.internal.RequestState;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class ResultActivity extends AppCompatActivity {
    private static final String TAG = "ResultActivity";

    private String requestId;
    private List<Uri> ocrUris = new ArrayList<>();
    private List<Uri> detectUris = new ArrayList<>();
    private List<Uri> allUris = new ArrayList<>();

    private ResultDataManager resultDataManager;

    private TextView statusText;
    private TextView progressText;
    private LinearLayout imagesContainer;

    private Handler uiHandler = new Handler();
    private Timer updateTimer;
    private static final int UPDATE_INTERVAL = 1000; // 1秒更新一次UI

    private boolean isRequestFinished = false;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.result_activity);

        resultDataManager = ResultDataManager.getInstance();

        // 设置Toolbar
        Toolbar toolbar = findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);
        if (getSupportActionBar() != null) {
            getSupportActionBar().setTitle("处理结果");
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        }

        // 获取传递的数据
        requestId = getIntent().getStringExtra(MainActivity.EXTRA_REQUEST_ID);
        Log.d(TAG, "页面跳转后，追踪requestId: " + requestId);
        ArrayList<String> ocrUriStrings = getIntent().getStringArrayListExtra(MainActivity.EXTRA_OCR_URIS);
        ArrayList<String> detectUriStrings = getIntent().getStringArrayListExtra(MainActivity.EXTRA_DETECT_URIS);

        if (ocrUriStrings != null) {
            for (String uriString : ocrUriStrings) {
                ocrUris.add(Uri.parse(uriString));
            }
        }

        if (detectUriStrings != null) {
            for (String uriString : detectUriStrings) {
                detectUris.add(Uri.parse(uriString));
            }
        }

        allUris.addAll(ocrUris);
        allUris.addAll(detectUris);

        Log.d(TAG, "ResultActivity onCreate, requestId: " + requestId + ", ocrUris: " + ocrUris.size() + ", detectUris: " + detectUris.size());

        // 初始化视图
        initViews();
        Log.d(TAG, "ResultActivity onCreate完成");

        // 启动定时更新
        startPeriodicUpdate();
    }

    private void initViews() {
        statusText = findViewById(R.id.status_text);
        progressText = findViewById(R.id.progress_text);
        imagesContainer = findViewById(R.id.images_container);

        // 创建图片对比项
        createImageItems();
    }

    private void createImageItems() {
        imagesContainer.removeAllViews();
        LayoutInflater inflater = LayoutInflater.from(this);

        for (int i = 0; i < allUris.size(); i++) {
            View itemView = inflater.inflate(R.layout.result_image_item, imagesContainer, false);

            TextView titleText = itemView.findViewById(R.id.image_title);
            ImageView originalImage = itemView.findViewById(R.id.original_image);
            ImageView resultImage = itemView.findViewById(R.id.result_image);
            ProgressBar loadingProgress = itemView.findViewById(R.id.loading_progress);
            TextView waitingText = itemView.findViewById(R.id.waiting_text);

            titleText.setText("图片 " + (i + 1));

            // 加载原图
            loadImageFromUri(allUris.get(i), originalImage);

            imagesContainer.addView(itemView);
        }
    }

    private void loadImageFromUri(Uri uri, ImageView imageView) {
        new Thread(() -> {
            try {
                InputStream inputStream = getContentResolver().openInputStream(uri);
                if (inputStream != null) {
                    Bitmap bitmap = BitmapFactory.decodeStream(inputStream);
                    inputStream.close();

                    runOnUiThread(() -> {
                        if (bitmap != null) {
                            imageView.setImageBitmap(bitmap);
                        }
                    });
                }
            } catch (Exception e) {
                Log.e(TAG, "加载图片失败: " + uri, e);
            }
        }).start();
    }

    private void updateUI() {
        // 每次都重新获取最新的requestState，确保数据是最新的
        RequestState requestState = resultDataManager.getRequestState(requestId);

        if (isRequestFinished) {
            // 请求已完成或超时，不再更新UI
            stopPeriodicUpdate();
            return;
        }

        if (requestState == null) {
            statusText.setText("正在初始化...");
            progressText.setText("准备中...");
            return;
        }

        // 更新状态文本
        if (requestState.updateAndGetCompletion()) {
            statusText.setText("处理完成");
            isRequestFinished = true;
            // 清理已完成的请求
            resultDataManager.cleanupRequest(requestId);
            stopPeriodicUpdate();
        } else if (requestState.isTimeout()) {
            statusText.setText("处理超时");
            isRequestFinished = true;
            // 清理超时的请求
            resultDataManager.cleanupRequest(requestId);
            stopPeriodicUpdate();
        } else {
            statusText.setText("正在处理中");
        }

        // 更新进度文本
        progressText.setText(requestState.getProgressText());

        // 更新图片结果
        updateImageResults(requestState);
    }

    private void updateImageResults(RequestState requestState) {
        Log.d(TAG, "更新图片结果");
        //输出当前requestState的内容
        if (requestState == null) return;

        Log.d(TAG, "requestState的内容: ");
        Log.d(TAG, "结果数：" + requestState.getReceivedResultCount());
        Log.d(TAG, "结果：" + requestState.getReceivedResults());

        for (int i = 0; i < imagesContainer.getChildCount(); i++) {
            View itemView = imagesContainer.getChildAt(i);
            ImageView resultImage = itemView.findViewById(R.id.result_image);
            ProgressBar loadingProgress = itemView.findViewById(R.id.loading_progress);
            TextView waitingText = itemView.findViewById(R.id.waiting_text);

            // 这里应该根据任务ID获取对应的结果，暂时用索引代替（目前taskId是从1开始自增的）
            String taskId = String.valueOf(i + 1);
            ResultItem resultItem = requestState.getResultItem(taskId);

            if (resultItem != null) {
                // 显示结果图
                Bitmap bitmap = convertResultItemToBitmap(resultItem);
                if (bitmap != null) {
                    resultImage.setImageBitmap(bitmap);
                    resultImage.setVisibility(View.VISIBLE);
                    loadingProgress.setVisibility(View.GONE);
                    waitingText.setVisibility(View.GONE);
                } else {
                    resultImage.setVisibility(View.GONE);
                    loadingProgress.setVisibility(View.VISIBLE);
                    waitingText.setVisibility(View.GONE);
                }
            } else {
                // 显示等待状态
                resultImage.setVisibility(View.GONE);
                if (requestState.isTimeout()) {
                    loadingProgress.setVisibility(View.GONE);
                    waitingText.setText("处理超时");
                    waitingText.setVisibility(View.VISIBLE);
                } else {
                    loadingProgress.setVisibility(View.VISIBLE);
                    waitingText.setVisibility(View.GONE);
                }
            }
        }
    }

    private Bitmap convertResultItemToBitmap(ResultItem resultItem) {
        Log.i(TAG, "开始转换图片字节数组为bitmap");
        try {
            if (resultItem.output_blob.length() > 0) {
                byte[] byteArray = new byte[resultItem.output_blob.length()];
                for (int i = 0; i < byteArray.length; i++) {
                    byteArray[i] = resultItem.output_blob.get_at(i);
                }
                return BitmapFactory.decodeByteArray(byteArray, 0, byteArray.length);
            }
        } catch (Exception e) {
            Log.e(TAG, "转换结果图片失败", e);
        }
        return null;
    }

    /**
     * 定时更新UI，默认每秒更新一次
     */
    private void startPeriodicUpdate() {
        updateTimer = new Timer();
        updateTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                uiHandler.post(() -> updateUI());
            }
        }, 0, UPDATE_INTERVAL);
    }

    private void stopPeriodicUpdate() {
        if (updateTimer != null) {
            updateTimer.cancel();
            updateTimer = null;
        }
    }


    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            finish();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        stopPeriodicUpdate();
        Log.d(TAG, "ResultActivity onDestroy");
    }
}