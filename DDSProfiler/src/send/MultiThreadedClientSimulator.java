package send;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MultiThreadedClientSimulator {
    //---------------------------配-置-区-域------------------------------

    // 线程数
    private static final int DEFAULT_THREAD_COUNT = 5;
    // 每个线程发送的请求数
    private static final int DEFAULT_REQUEST_COUNT_PER_THREAD = 5;
    // 任务数范围
    private static final int DEFAULT_MIN_TASKS_PER_REQUEST = 1;
    private static final int DEFAULT_MAX_TASKS_PER_REQUEST = 10;
    // 请求间隔范围
    private static final int DEFAULT_MIN_REQUEST_INTERVAL_MS = 50;
    private static final int DEFAULT_MAX_REQUEST_INTERVAL_MS = 500;

// -----------------------------------------------------------------------
    
    public static void main(String[] args) {
        // 确保logs目录存在
        File logsDir = new File("logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }
        
        // 直接使用默认配置值
        int threadCount = DEFAULT_THREAD_COUNT;
        int requestCountPerThread = DEFAULT_REQUEST_COUNT_PER_THREAD;
        int minTasksPerRequest = DEFAULT_MIN_TASKS_PER_REQUEST;
        int maxTasksPerRequest = DEFAULT_MAX_TASKS_PER_REQUEST;
        int minRequestIntervalMs = DEFAULT_MIN_REQUEST_INTERVAL_MS;
        int maxRequestIntervalMs = DEFAULT_MAX_REQUEST_INTERVAL_MS;
        
        System.out.println("Starting multi-threaded client simulator without thread pool with " + threadCount + " threads");
        System.out.println("Each thread will send " + requestCountPerThread + " requests");
        System.out.println("Tasks per request: " + minTasksPerRequest + " to " + maxTasksPerRequest);
        System.out.println("Request interval: " + minRequestIntervalMs + " to " + maxRequestIntervalMs + " ms");
        
        // 存储所有线程的列表
        List<Thread> threads = new ArrayList<>();
        
        // 为每个线程创建任务
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                SendService sender = null;
                try {
                    // 为每个线程创建唯一的客户端ID
                    String clientId = "client-" + 
                            Integer.toUnsignedString(new Random().nextInt(), 36) + "-thread" + threadId;
                    
                    // 使用带配置的构造函数
                    sender = new SendService(clientId, minTasksPerRequest, maxTasksPerRequest,
                            minRequestIntervalMs, maxRequestIntervalMs);
                    
                    // 发送请求
                    sender.sendMixedRequests(requestCountPerThread);
                    
                    System.out.println("Thread " + threadId + " finished sending requests");
                } catch (Exception e) {
                    System.err.println("Error in thread " + threadId + ": " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    if (sender != null) {
                        sender.close();
                    }
                }
            });
            
            // 设置线程名称
            thread.setName("ClientSimulatorThread-" + threadId);
            threads.add(thread);
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.err.println("Interrupted while waiting for thread " + thread.getName() + " to complete");
                e.printStackTrace();
            }
        }

        System.out.println("All threads completed. Multi-threaded client simulation finished.");
    }
}