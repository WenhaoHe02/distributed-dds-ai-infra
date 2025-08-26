import com.zrdds.infrastructure.StatusKind;
import com.zrdds.simpleinterface.DDSIF;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.subscription.DataReader;
import com.zrdds.subscription.SimpleDataReaderListener;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.SampleInfo;
import com.zrdds.topic.Topic;
import com.zrdds.publication.Publisher;
import com.zrdds.subscription.Subscriber;

import ai.*; // WorkerResult / WorkerResultSeq / WorkerResultDataReader / AggregatedResult* / *TypeSupport

/**
 * Aggregator:
 * - Subscribe: workerResult
 * - Publish  : AggregatedResult
 * - 汇总 Worker 结果并发送
 */
public class Aggregator {

    private static final int DOMAIN_ID = 100;
    private static final String FACTORY_PROFILE     = "non_rio";
    private static final String PARTICIPANT_PROFILE = "tcp_dp";

    private static final String WORKER_READER_QOS   = "non_zerocopy_reliable";
    private static final String AGG_WRITER_QOS      = "non_zerocopy_reliable";

    private static final String TOPIC_WORKER_RESULT = "workerResult";
    private static final String TOPIC_AGG_RESULT    = "AggregatedResult";

    // 仅接收特定 client_id 的数据
    private static final String FILTER_EXPRESSION = "client_id = %0";
    private static final String[] FILTER_PARAMS = new String[]{"client_123"};

    // Listener：收到 WorkerResult 就汇总并发布 AggregatedResult
    private static class WorkerResultListener extends SimpleDataReaderListener<WorkerResult, WorkerResultSeq, WorkerResultDataReader> {
        private final AggregatedResultDataWriter aggWriter;

        WorkerResultListener(AggregatedResultDataWriter writer) {
            this.aggWriter = writer;
        }

        @Override
        public void on_process_sample(DataReader reader, WorkerResult sample, SampleInfo info) {
            try {
                // 构造 AggregatedResult
                AggregatedResult agg = new AggregatedResult();
                agg.request_id = sample.results.get_at(0).task.request_id;
                agg.client_id  = sample.results.get_at(0).task.client_id;
                agg.status     = "AGGREGATED"; // 汇总状态
                agg.error_message = "";

                // 汇总结果：简单把 WorkerResult 包装到 AggregatedResult.results
                agg.results = new SingleResultSeq();
                if (sample.results != null && sample.results.length() > 0) {
                    for (int i = 0; i < sample.results.length(); i++) {
                        SingleResult r = sample.results.get_at(i);
                        if (r != null) agg.results.append(r);
                    }
                }

                ReturnCode_t rc = aggWriter.write(agg, InstanceHandle_t.HANDLE_NIL_NATIVE);
                if (rc != ReturnCode_t.RETCODE_OK) {
                    System.err.println("[Aggregator] Write AggregatedResult failed: " + rc);
                } else {
                    System.out.println("[Aggregator] AggregatedResult sent for request: " + agg.request_id);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        @Override
        public void on_data_arrived(DataReader reader, Object sample, SampleInfo info) {
            // 可选
        }
    }

    public static void main(String[] args) throws Exception {
        // 1) 初始化 DDS
        DomainParticipantFactory pf = DDSIF.init("ZRDDS_QOS_PROFILES.xml", FACTORY_PROFILE);
        if (pf == null) { System.err.println("DDSIF.init failed"); return; }

        DomainParticipant dp = DDSIF.create_dp(DOMAIN_ID, PARTICIPANT_PROFILE);
        if (dp == null) { System.err.println("create_dp failed"); return; }

        WorkerResultTypeSupport workerTS = (WorkerResultTypeSupport) WorkerResultTypeSupport.get_instance();
        AggregatedResultTypeSupport aggTS = (AggregatedResultTypeSupport) AggregatedResultTypeSupport.get_instance();

        if (workerTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK
                || aggTS.register_type(dp, null) != ReturnCode_t.RETCODE_OK) {
            System.err.println("register type failed");
            return;
        }

        // 2) 创建 Topic
        Topic workerTopic = dp.create_topic(
                TOPIC_WORKER_RESULT,
                workerTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE
        );

        Topic aggTopic = dp.create_topic(
                TOPIC_AGG_RESULT,
                aggTS.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT,
                null,
                StatusKind.STATUS_MASK_NONE
        );

        if (workerTopic == null || aggTopic == null) {
            System.err.println("create topic failed");
            return;
        }

        // 3) 创建 Publisher / Subscriber
        Publisher pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        Subscriber sub = dp.create_subscriber(DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // 4) 创建 DataWriter / DataReader
        AggregatedResultDataWriter aggWriter = (AggregatedResultDataWriter)
                DDSIF.pub_topic(dp, TOPIC_AGG_RESULT, aggTS, AGG_WRITER_QOS, null);

        WorkerResultDataReader workerReader = (WorkerResultDataReader)
                DDSIF.sub_topic(dp, TOPIC_WORKER_RESULT, workerTS, WORKER_READER_QOS,
                        new WorkerResultListener(aggWriter));

        if (aggWriter == null || workerReader == null) {
            System.err.println("writer/reader creation failed");
            return;
        }

        System.out.println("Aggregator started. Press ENTER to exit.");
        System.in.read();
        DDSIF.Finalize();
    }
}
