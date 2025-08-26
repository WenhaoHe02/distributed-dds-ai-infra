package worker;

import com.zrdds.infrastructure.*;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;
import com.zrdds.topic.Topic;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;

import java.util.concurrent.*;

import data_structure.*; // TaskList*/WorkerResult*/SingleResult*/SingleTask* + *TypeSupport + *DataReader/*DataWriter

import static worker.ModelRunner.runSingleTask;

/**
 * Worker（无批处理：单任务执行，但支持排队）
 * - 订阅  : "inference/tasklist" (TaskList)
 * - 入队  : 将 TaskList.tasks 中的每个 SingleTask 放入阻塞队列
 * - 消费  : 单线程从队列取出顺序执行 ModelRunner(task)
 * - 发布  : 每个任务生成一个 WorkerResult（只含 1 个 SingleResult）到 "inference/worker_result"
 * 使用默认 QoS，不依赖 XML。确保与 Dispatcher 在同一 DOMAIN_ID。
 */
public class Worker {

    static final int DOMAIN_ID = 100;
    static final String TOPIC_TASKLIST = "inference/tasklist";
    static final String TOPIC_RESULT   = "inference/worker_result"; // Worker -> Dispatcher/Aggregator

    // 队列容量（可按机器内存调大/调小）
    private static final int QUEUE_CAPACITY = 100;

    // 任务队列（单消费者线程顺序执行）
    private final BlockingQueue<SingleTask> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    private WorkerResultDataWriter resultWriter;

    // ====== 监听 TaskList：拆分入队（非阻塞 offer，满了打印告警） ======
    final class TaskListListener extends SimpleDataReaderListener<TaskList, TaskListSeq, TaskListDataReader> {
        @Override public void on_process_sample(DataReader reader, TaskList tl, SampleInfo info) {
            try {
                int n = (tl.tasks != null) ? tl.tasks.length() : 0;
                for (int i = 0; i < n; i++) {
                    SingleTask t = tl.tasks.get_at(i);
                    if (!queue.offer(t)) {
                        System.err.println("[WorkerQueue] queue full, drop task_id=" + t.task_id + ", request_id=" + t.request_id);
                    }
                }
                System.out.println("[WorkerQueue] enqueued " + n + " tasks. current queue size=" + queue.size());
            } catch (Throwable e) { e.printStackTrace(); }
        }
        @Override public void on_data_arrived(DataReader r, Object s, SampleInfo i) {}
    }

    // ====== 单线程消费者：逐个执行并发布结果 ======
    private void startConsumer() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    SingleTask task = queue.take(); // 阻塞等待
                    long t0 = System.nanoTime();

                    // 调模型
                    SingleResult sr = runSingleTask(task);

                    // 打包为 WorkerResult（只含 1 条）并发布
                    WorkerResult wr = new WorkerResult();
                    wr.result_num = 1;
                    wr.results = new SingleResultSeq();
                    wr.results.ensure_length(1, 1);
                    wr.results.set_at(0, sr);

                    ReturnCode_t rc = resultWriter.write(wr, InstanceHandle_t.HANDLE_NIL_NATIVE);
                    if (rc != ReturnCode_t.RETCODE_OK) {
                        System.err.println("[WorkerQueue] publish result failed rc=" + rc + " task_id=" + task.task_id);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }, "worker-consumer");
        t.setDaemon(true);
        t.start();
    }

    // ====== 模型执行占位实现：原样回显输入 Bytes ======

    public static void main(String[] args) throws Exception {
        // 1) 工厂/参与者（默认 QoS）
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        DomainParticipant dp = dpf.create_participant(
                DOMAIN_ID,
                DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE);
        if (dp == null) { System.err.println("create dp failed"); return; }

        // 2) 注册类型
        TaskListTypeSupport     tlTS = (TaskListTypeSupport)     TaskListTypeSupport.get_instance();
        WorkerResultTypeSupport wrTS = (WorkerResultTypeSupport) WorkerResultTypeSupport.get_instance();
        if (tlTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK ||
                wrTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed"); return;
        }

        // 3) 主题
        Topic taskTopic = dp.create_topic(TOPIC_TASKLIST, tlTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Topic resTopic  = dp.create_topic(TOPIC_RESULT,   wrTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (taskTopic == null || resTopic == null) { System.err.println("create topic failed"); return; }

        // 4) Pub/Sub
        Publisher  pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 5) Writer/Reader（默认 QoS）
        WorkerResultDataWriter resWriter = (WorkerResultDataWriter)
                pub.create_datawriter(resTopic, Publisher.DATAWRITER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        TaskListDataReader     tlReader  = (TaskListDataReader)
                sub.create_datareader(taskTopic, Subscriber.DATAREADER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        Worker worker = new Worker();
        worker.resultWriter = resWriter;

        tlReader.set_listener(worker.new TaskListListener(), StatusKind.STATUS_MASK_ALL);
        worker.startConsumer();

        System.out.println("WorkerQueue started. Press ENTER to exit.");
        System.in.read();

        dp.delete_contained_entities();
        dpf.delete_participant(dp);
        DomainParticipantFactory.finalize_instance();
    }
}

