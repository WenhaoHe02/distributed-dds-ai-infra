import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GlobalResourceManager {
    // 只有一线程读一线程写，故不再另外加锁，仅使用volatile
    private volatile String filePath;
    private volatile int requestCount = 0;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock filePathLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock requestCountLock = new ReentrantReadWriteLock();

    // 私有构造函数，外部不能 new
    private GlobalResourceManager(String initialFilePath) {
        this.filePath = initialFilePath;
    }

    // 静态内部类持有单例
    private static class Holder {
        private static final GlobalResourceManager INSTANCE =
                new GlobalResourceManager("data.txt"); // 可以改成配置文件路径
    }

    // 全局访问点
    public static GlobalResourceManager getInstance() {
        return Holder.INSTANCE;
    }

    // ======= 变量访问 =======
    public String getFilePath() {
        filePathLock.readLock().lock();
        try {
            return filePath;
        } finally {
            filePathLock.readLock().unlock();
        }
    }

    public void setFilePath(String filePath) {
        filePathLock.writeLock().lock();
        try {
            this.filePath = filePath;
        } finally {
            filePathLock.writeLock().unlock();
        }

    }

    public int getRequestCount() {
        requestCountLock.readLock().lock();
        try {
            return requestCount;
        } finally {
            requestCountLock.readLock().unlock();
        }
    }

    public void setRequestCount(int requestCount) {
        requestCountLock.writeLock().lock();
        try {
            this.requestCount = requestCount;
        } finally {
            requestCountLock.writeLock().unlock();
        }
    }

    // ======= 文件访问锁 =======
    public void acquireReadLock() {
        rwLock.readLock().lock();
    }

    public void releaseReadLock() {
        rwLock.readLock().unlock();
    }

    public void acquireWriteLock() {
        rwLock.writeLock().lock();
    }

    public void releaseWriteLock() {
        rwLock.writeLock().unlock();
    }
}
