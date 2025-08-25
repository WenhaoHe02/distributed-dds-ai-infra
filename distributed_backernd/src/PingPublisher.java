import ai.Ping;
import ai.PingDataWriter;
import ai.PingTypeSupport;
import com.zrdds.*;
import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.publication.DataWriter;
import com.zrdds.publication.Publisher;
import com.zrdds.topic.Topic;

import static com.zrdds.domain.DomainParticipant.PUBLISHER_QOS_DEFAULT;
import static com.zrdds.domain.DomainParticipant.TOPIC_QOS_DEFAULT;
import static com.zrdds.domain.DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT;
import static com.zrdds.publication.Publisher.DATAWRITER_QOS_DEFAULT;

public class PingPublisher {
    public static void main(String[] args) {
        // 1. 创建参与者
        DomainParticipantFactory dpf = DomainParticipantFactory.get_instance();
        DomainParticipant dp = dpf.create_participant(
                0, PARTICIPANT_QOS_DEFAULT.get(), null, 0);

        // 2. 注册类型
        PingTypeSupport ts = new PingTypeSupport();
        ts.register_type(dp, "Ping");

        // 3. 创建 Topic
        Topic topic = dp.create_topic("PingTopic", "Ping",
                TOPIC_QOS_DEFAULT.get(), null, 0);

        // 4. Publisher & Writer
        Publisher pub = dp.create_publisher(PUBLISHER_QOS_DEFAULT.get(), null, 0);
        DataWriter dw = pub.create_datawriter(topic, DATAWRITER_QOS_DEFAULT.get(), null, 0);
        PingDataWriter writer = PingDataWriterHelper.narrow(dw);

        // 5. 发送一条数据
        Ping msg = new Ping();
        msg.msg = "Hello DDS from IDEA!";
        writer.write(msg, HANDLE_NIL.value);

        System.out.println("Ping sent.");
    }
}
