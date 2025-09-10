package receive;

import static java.lang.Thread.sleep;

public class main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Thread: " + Thread.currentThread().getName());
        ReceiveService receiveService = new ReceiveService();
        receiveService.initDDS();
        while(true){
            System.out.println("[receive.main] 等待数据");
            sleep(5000);
        }
//        // TODO: 如何在程序正常/异常退出都正确进行清理？py代码是否应该补充相关的


//        receive.ReadSendLog readSendLog = new receive.ReadSendLog();
//        readSendLog.updateRead();
    }
}
