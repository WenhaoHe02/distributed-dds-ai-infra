import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.io.File;

public class MultiThreadedSendService {
    private static final int DEFAULT_THREAD_COUNT = 5;
    private static final int DEFAULT_REQUEST_COUNT = 20;
    
    public static void main(String[] args) {
        // 确保logs目录存在
        File logsDir = new File("logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }
        
        int threadCount = Integer.parseInt(System.getProperty("thread.count", String.valueOf(DEFAULT_THREAD_COUNT)));
        int requestCount = Integer.parseInt(System.getProperty("request.count", String.valueOf(DEFAULT_REQUEST_COUNT)));
        
        System.out.println("Starting multi-threaded request sender with " + threadCount + " threads, each sending " + requestCount + " requests");
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        // 存储所有线程任务的列表
        List<Runnable> tasks = new ArrayList<>();
        
        // 为每个线程创建任务
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Runnable task = () -> {
                SendService sender = null;
                try {
                    // 为每个线程创建唯一的客户端ID
                    String clientId = System.getProperty("client.id", "client-" + 
                            Integer.toUnsignedString(new Random().nextInt(), 36)) + "-thread" + threadId;
                    
                    sender = new SendService(clientId);
                    
                    // 发送请求
                    sender.sendMixedRequests(requestCount);
                    
                    System.out.println("Thread " + threadId + " finished sending requests");
                } catch (Exception e) {
                    System.err.println("Error in thread " + threadId + ": " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    if (sender != null) {
                        sender.close();
                    }
                }
            };
            tasks.add(task);
        }
        
        // 提交所有任务到线程池
        for (Runnable task : tasks) {
            executor.submit(task);
        }
        
        // 关闭线程池并等待所有任务完成
        executor.shutdown();
        try {
            if (!executor.awaitTermination(threadCount * requestCount * 2, TimeUnit.SECONDS)) {
                System.err.println("Tasks did not complete in time, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for tasks to complete");
            executor.shutdownNow();
        }
        
        System.out.println("All threads completed. Multi-threaded request sending finished.");
    }
}