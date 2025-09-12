import common.GlobalResourceManager;
import receive.ReceiveService;
import send.SendService;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class Main {

    // 多线程配置
    private static final int DEFAULT_THREAD_COUNT = 2;
    private static final int DEFAULT_REQUEST_COUNT_PER_THREAD = 5;
    private static final int DEFAULT_MIN_TASKS_PER_REQUEST = 2;
    private static final int DEFAULT_MAX_TASKS_PER_REQUEST = 2;
    private static final int DEFAULT_MIN_REQUEST_INTERVAL_MS = 50;
    private static final int DEFAULT_MAX_REQUEST_INTERVAL_MS = 500;

    private static GlobalResourceManager resourceManager = GlobalResourceManager.getInstance();


    public static void main(String[] args) {
        System.out.println("[MAIN] 进程启动");

        // 确保logs目录存在
        File logsDir = new File("logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }

        // 1. 初始化接收端 ReceiveService

        System.out.println("[MAIN] 初始化接收端...");
        System.out.println(resourceManager.getTestEnd());
        ReceiveService receiveService = new ReceiveService();
        receiveService.initDDS(); // 内部挂载 ListenerDataReaderListener



        // 2. 初始化多线程发送端


        System.out.println("[MAIN] 初始化多线程发送端...");
        List<Thread> sendThreads = new ArrayList<>();
        List<SendService> sendServices = new ArrayList<>();
        
        // 创建多个发送线程
        for (int i = 0; i < DEFAULT_THREAD_COUNT; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                SendService sender = null;
                try {
                    // 为每个线程创建唯一的客户端ID
                    String clientId = "client-" + 
                            Integer.toUnsignedString(new Random().nextInt(), 36) + "-thread" + threadId;
                    
                    // 使用带配置的构造函数
                    sender = new SendService(clientId, DEFAULT_MIN_TASKS_PER_REQUEST, DEFAULT_MAX_TASKS_PER_REQUEST,
                            DEFAULT_MIN_REQUEST_INTERVAL_MS, DEFAULT_MAX_REQUEST_INTERVAL_MS);
                    
                    // 保存发送服务引用以便后续清理
                    synchronized (sendServices) {
                        sendServices.add(sender);
                    }
                    
                    // 发送请求
                    sender.sendMixedRequests(DEFAULT_REQUEST_COUNT_PER_THREAD);
                    
                    System.out.println("[MAIN] Thread " + threadId + " finished sending requests");
                } catch (Exception e) {
                    System.err.println("[MAIN] Error in thread " + threadId + ": " + e.getMessage());
                    e.printStackTrace();
                }
            });
            
            // 设置线程名称
            thread.setName("SendThread-" + threadId);
            sendThreads.add(thread);
        }

        // --------------------------------------------
        // 3. 启动所有发送线程
        // --------------------------------------------
        System.out.println("[MAIN] 启动所有发送线程...");
        for (Thread thread : sendThreads) {
            thread.start();
        }

        // --------------------------------------------
        // 4. 保持接收端运行 & 等待发送完成
        // --------------------------------------------
        try {
            // 等待所有发送线程结束
            for (Thread thread : sendThreads) {
                thread.join();
            }

            System.out.println("[MAIN] 所有发送线程已完成");
            System.out.println("[MAIN] 接收端继续运行，等待数据...");
            System.out.println(resourceManager.getTestEnd());
            while(!resourceManager.getTestEnd());

            System.out.println("[MAIN] 测试完成，准备清理资源");


        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            // --------------------------------------------
            // 5. 清理资源
            // --------------------------------------------
            System.out.println("[MAIN] 清理资源...");
            // 关闭所有发送端服务
            for (SendService service : sendServices) {
                if (service != null) {
                    service.close();
                }
            }
            
            if (receiveService != null) {
                receiveService.clean(); // 清理接收端DDS资源
            }
        }

        System.out.println("[MAIN] 进程退出");
    }
}