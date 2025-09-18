package com.example.ocrclient;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.MenuItem;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ScrollView;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class ResultActivity extends AppCompatActivity implements ResultUpdateListener {
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

    // 记录已经渲染过的任务ID，用于增量更新
    private Set<String> renderedTaskIds = new HashSet<>();

    // 全屏图片查看相关
    private View fullscreenLayout;
    private ImageView fullscreenImage;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.result_activity);

        resultDataManager = ResultDataManager.getInstance();
        resultDataManager.addResultUpdateListener(this);
        Log.d(TAG, "ResultUpdateListener registered");

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
            ScrollView ocrScrollView = itemView.findViewById(R.id.ocr_scroll_view);

            titleText.setText("图片 " + (i + 1));

            // 加载原图
            loadImageFromUri(allUris.get(i), originalImage);

            // 设置点击事件，用于全屏查看图片
            final int index = i;
            originalImage.setOnClickListener(v -> showFullscreenImage(allUris.get(index)));
            resultImage.setOnClickListener(v -> showResultImageFullscreen(index));

            // 为OCR ScrollView设置触摸事件处理，解决滚动冲突
            if (ocrScrollView != null) {
                ocrScrollView.setOnTouchListener(new View.OnTouchListener() {
                    @Override
                    public boolean onTouch(View v, MotionEvent event) {
                        // 当用户触摸OCR文本区域时，请求父视图不要拦截触摸事件
                        v.getParent().requestDisallowInterceptTouchEvent(true);
                        
                        // 处理滚动到边界时的情况
                        switch (event.getAction() & MotionEvent.ACTION_MASK) {
                            case MotionEvent.ACTION_UP:
                            case MotionEvent.ACTION_CANCEL:
                                // 触摸结束时，允许父视图重新拦截触摸事件
                                v.getParent().requestDisallowInterceptTouchEvent(false);
                                break;
                        }
                        
                        return false; // 让ScrollView正常处理滚动事件
                    }
                });
            }

            imagesContainer.addView(itemView);
        }

        // 清空已渲染任务ID记录，因为重新创建了所有视图
        renderedTaskIds.clear();
        Log.d(TAG, "已清空已渲染任务ID记录");
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
        updateUI(false);
    }

    // 更新UI方法，添加forceUpdateImages参数控制是否强制更新图片
    private void updateUI(boolean forceUpdateImages) {
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
//            // 清理已完成的请求
//            resultDataManager.cleanupRequest(requestId);
            stopPeriodicUpdate();
        } else if (requestState.isAnyTimeout()) { // 使用新的超时检测方法
            statusText.setText("处理超时");
            isRequestFinished = true;
//            // 清理超时的请求
//            resultDataManager.cleanupRequest(requestId);
            stopPeriodicUpdate();
        } else {
            statusText.setText("正在处理中");
        }

        // 更新进度文本
        progressText.setText(requestState.getProgressText());

        // 只有在强制更新或主动刷新时才更新图片结果
        // 当检测到超时时，也需要更新图片结果以显示超时状态
        if (forceUpdateImages || (requestState != null && requestState.isAnyTimeout())) {
            updateImageResults(requestState);
        }
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
            ScrollView ocrScrollView = itemView.findViewById(R.id.ocr_scroll_view);
            TextView ocrText = itemView.findViewById(R.id.ocr_text);

            // 这里应该根据任务ID获取对应的结果，暂时用索引代替（目前taskId是从1开始自增的）
            String taskId = String.valueOf(i + 1);
            Log.d(TAG, "任务ID：" + taskId);

            // 如果该任务ID已经渲染过，则跳过
            if (renderedTaskIds.contains(taskId)) {
                Log.d(TAG, "任务ID " + taskId + " 已经渲染过，跳过");
                continue;
            }

            ResultItem resultItem = requestState.getResultItem(taskId);
            Log.d(TAG, "结果项：" + resultItem);

            if (resultItem != null) {
                Log.d(TAG, "结果项不为空，开始处理");
                // 显示结果图
                Bitmap bitmap = convertResultItemToBitmap(resultItem);
                Log.d(TAG, "Bitmap转换结果: " + (bitmap != null ? "成功" : "失败"));
                if (bitmap != null) {
                    resultImage.setImageBitmap(bitmap);
                    resultImage.setVisibility(View.VISIBLE);
                    loadingProgress.setVisibility(View.GONE);
                    waitingText.setVisibility(View.GONE);

                    // 显示OCR文字（如果存在）
                    Log.d(TAG, "OCR文字：" + resultItem.texts.toString());
                    Log.d(TAG, "OCR 文字长度：" + resultItem.texts.length());
                    if (resultItem.texts != null && resultItem.texts.length() > 0) {
                        Log.d(TAG, "OCR文字存在，OCR文字：" + resultItem.texts);
                        StringBuilder textBuilder = new StringBuilder();
                        for (int j = 0; j < resultItem.texts.length(); j++) {
                            if (j > 0) textBuilder.append("\n");
                            textBuilder.append(resultItem.texts.get_at(j));
                        }
                        ocrText.setText(textBuilder.toString());
                        ocrScrollView.setVisibility(View.VISIBLE);
                    } else {
                        Log.d(TAG, "OCR文字不存在");
                        ocrScrollView.setVisibility(View.GONE);
                    }

                    // 标记该任务ID已经渲染
                    Log.d(TAG, "标记任务ID " + taskId + " 已渲染");
                    renderedTaskIds.add(taskId);
                } else {
                    Log.d(TAG, "Bitmap为空，不标记任务ID");
                    resultImage.setVisibility(View.GONE);
                    loadingProgress.setVisibility(View.VISIBLE);
                    waitingText.setVisibility(View.GONE);
                    ocrScrollView.setVisibility(View.GONE);
                }
            } else {
                Log.d(TAG, "结果项为空");
                // 显示等待状态
                resultImage.setVisibility(View.GONE);
                if (requestState.isAnyTimeout()) { // 使用新的超时检测方法
                    Log.d(TAG, "检测到超时状态");
                    loadingProgress.setVisibility(View.GONE);
                    waitingText.setText("处理超时");
                    waitingText.setVisibility(View.VISIBLE);
                } else {
                    Log.d(TAG, "未超时，显示加载状态");
                    loadingProgress.setVisibility(View.VISIBLE);
                    waitingText.setVisibility(View.GONE);
                }
                ocrScrollView.setVisibility(View.GONE);
            }
        }
        Log.d(TAG, "图片结果更新完成");
    }

    private Bitmap convertResultItemToBitmap(ResultItem resultItem) {
        Log.i(TAG, "开始转换图片字节数组为bitmap");
        try {
            if (resultItem.output_blob != null && resultItem.output_blob.value != null) {
                Log.d(TAG, "output_blob不为空，长度: " + resultItem.output_blob.value.length());
                if (resultItem.output_blob.value.length() > 0) {
                    byte[] byteArray = new byte[resultItem.output_blob.value.length()];
                    for (int i = 0; i < byteArray.length; i++) {
                        byteArray[i] = resultItem.output_blob.value.get_at(i);
                    }
                    Log.d(TAG, "字节数组准备完成，长度: " + byteArray.length);

                    Bitmap bitmap = BitmapFactory.decodeByteArray(byteArray, 0, byteArray.length);
                    Log.d(TAG, "Bitmap转换完成，结果: " + (bitmap != null ? "成功" : "失败"));
                    return bitmap;
                } else {
                    Log.w(TAG, "output_blob长度为0");
                }
            } else {
                Log.w(TAG, "output_blob或其value为空");
            }
        } catch (Exception e) {
            Log.e(TAG, "转换结果图片失败", e);
        } catch (OutOfMemoryError e) {
            Log.e(TAG, "转换结果图片时内存不足", e);
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
                uiHandler.post(() -> updateUI(false)); // 定时更新不强制更新图片
            }
        }, 0, UPDATE_INTERVAL);
    }

    private void stopPeriodicUpdate() {
        if (updateTimer != null) {
            updateTimer.cancel();
            updateTimer = null;
        }
    }

    /**
     * 显示全屏图片
     */
    private void showFullscreenImage(Uri imageUri) {
        // 创建全屏布局
        fullscreenLayout = getLayoutInflater().inflate(R.layout.fullscreen_image, null);
        fullscreenImage = fullscreenLayout.findViewById(R.id.fullscreen_image);

        // 设置点击事件，用于关闭全屏查看
        fullscreenLayout.setOnClickListener(v -> closeFullscreenImage());

        // 添加到根布局
        ViewGroup rootView = findViewById(android.R.id.content);
        rootView.addView(fullscreenLayout);

        // 加载并显示图片
        loadImageFromUri(imageUri, fullscreenImage);
    }

    /**
     * 显示结果图全屏
     */
    private void showResultImageFullscreen(int index) {
        RequestState requestState = resultDataManager.getRequestState(requestId);
        if (requestState == null) return;

        String taskId = String.valueOf(index + 1);
        ResultItem resultItem = requestState.getResultItem(taskId);
        if (resultItem == null) return;

        Bitmap bitmap = convertResultItemToBitmap(resultItem);
        if (bitmap == null) return;

        // 创建全屏布局
        fullscreenLayout = getLayoutInflater().inflate(R.layout.fullscreen_image, null);
        fullscreenImage = fullscreenLayout.findViewById(R.id.fullscreen_image);
        fullscreenImage.setImageBitmap(bitmap);

        // 设置点击事件，用于关闭全屏查看
        fullscreenLayout.setOnClickListener(v -> closeFullscreenImage());

        // 添加到根布局
        ViewGroup rootView = findViewById(android.R.id.content);
        rootView.addView(fullscreenLayout);
    }

    /**
     * 关闭全屏图片查看
     */
    private void closeFullscreenImage() {
        if (fullscreenLayout != null) {
            ViewGroup rootView = findViewById(android.R.id.content);
            rootView.removeView(fullscreenLayout);
            fullscreenLayout = null;
            fullscreenImage = null;
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

    // 实现ResultUpdateListener接口的方法
    @Override
    public void onResultUpdated(String updatedRequestId) {
        Log.d(TAG, "结果页面的onResultUpdate触发");
        // 只处理当前页面关注的requestId
        if (updatedRequestId.equals(requestId)) {
            Log.d(TAG, "是结果页面关注的id，开始更新页面");
            uiHandler.post(() -> updateUI(true)); // 主动刷新时强制更新图片
        }
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        stopPeriodicUpdate();
        // 移除监听器避免内存泄漏
        if (resultDataManager != null) {
            resultDataManager.removeResultUpdateListener(this);
            Log.d(TAG, "ResultUpdateListener removed");
        }
        Log.d(TAG, "ResultActivity onDestroy");
    }
}