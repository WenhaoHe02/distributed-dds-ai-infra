import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class GlobalResourceManager {
    private volatile String filePath;
    private volatile int requestCount = 0;
    private final ReentrantReadWriteLock fileLock = new ReentrantReadWriteLock();
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
        fileLock.readLock().lock();
        try {
            return filePath;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    public void setFilePath(String filePath) {
        fileLock.writeLock().lock();
        try {
            this.filePath = filePath;
        } finally {
            fileLock.writeLock().unlock();
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
        fileLock.readLock().lock();
    }

    public void releaseReadLock() {
        fileLock.readLock().unlock();
    }

    public void acquireWriteLock() {
        fileLock.writeLock().lock();
    }

    public void releaseWriteLock() {
        fileLock.writeLock().unlock();
    }

    public <T> T withReadLock(Supplier<T> action) {
        fileLock.readLock().lock();
        try {
            return action.get();
        } finally {
            fileLock.readLock().unlock();
        }
    }

    public <T> T withWriteLock(Supplier<T> action) {
        fileLock.writeLock().lock();
        try {
            return action.get();
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    public void withReadLock(Runnable action) {
        fileLock.readLock().lock();
        try {
            action.run();
        } finally {
            fileLock.readLock().unlock();
        }
    }

    public void withWriteLock(Runnable action) {
        fileLock.writeLock().lock();
        try {
            action.run();
        } finally {
            fileLock.writeLock().unlock();
        }
    }

}
