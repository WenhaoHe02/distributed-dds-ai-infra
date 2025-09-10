package forTest;

import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.InstanceHandle_t;
import com.zrdds.infrastructure.ReturnCode_t;
import com.zrdds.infrastructure.StatusKind;
import com.zrdds.publication.DataWriter;
import com.zrdds.publication.Publisher;
import com.zrdds.topic.Topic;
import data_structure.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SendResultUpdate {
    public static void main(String[] args) {
        loadLibrary();
        ReturnCode_t rtn;

        // 域号
        int domian_id = 100;
        // 创建域参与者
        DomainParticipant dp = DomainParticipantFactory.get_instance().create_participant(
                domian_id, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        if (dp == null) {
            System.out.println("create dp failed");
            return;
        }

        // 注册数据类型
        ResultUpdateTypeSupport ResultUpdate = (ResultUpdateTypeSupport) ResultUpdateTypeSupport.get_instance();
        rtn = ResultUpdate.register_type(dp, null);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("register type failed");
            return;
        }

        // 创建主题
        Topic tp = dp.create_topic("inference/result_update", ResultUpdate.get_type_name(),
                DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
        if (tp == null) {
            System.out.println("create tp failed");
            return;
        }

        // 创建发布者
        Publisher pub = dp.create_publisher(DomainParticipant.PUBLISHER_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        if (pub == null) {
            System.out.println("create pub failed");
            return;
        }

        // 创建数据写者
        DataWriter _dw = pub.create_datawriter(tp, Publisher.DATAWRITER_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
        ResultUpdateDataWriter dw = (ResultUpdateDataWriter) (_dw);
        if (dw == null) {
            System.out.println("create dw failed");
            return;
        }

        // 初始化数据
        ResultUpdate data = new ResultUpdate();
        data.request_id = "123";
        data.client_id = "client03";

        ResultItem task = new ResultItem();
        task.task_id = "t002";

        data.items.ensure_length(1, 1);
        data.items.set_at(0, task);


//        while (true) {
//
//            rtn = dw.write(data, InstanceHandle_t.HANDLE_NIL_NATIVE);
//            if (rtn != ReturnCode_t.RETCODE_OK) {
//                System.out.println("write failed");
//            }
//            tSleep(2000);
//        }

        rtn = dw.write(data, InstanceHandle_t.HANDLE_NIL_NATIVE);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("write failed");
            return;
        }
        System.out.println("write success");

        // 释放DDS资源
        rtn = dp.delete_contained_entities();
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("dp delete contained entities failed");
            return;
        }

        rtn = DomainParticipantFactory.get_instance().delete_participant(dp);
        if (rtn != ReturnCode_t.RETCODE_OK) {
            System.out.println("dpf delete dp failed");
            return;
        }
        DomainParticipantFactory.finalize_instance();
    }

    // 等待接口
    public static void tSleep(int ti) {
        try {
            Thread.sleep(ti);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 已加载库标识
    private static boolean hasLoad = false;

    // 加载DDS库
    public static void loadLibrary() {
        if (!hasLoad) {
            System.loadLibrary("ZRDDS_JAVA");
            hasLoad = true;
        }
    }

    // 测试用图片链接
    private static final String IMAGE_PATH = "C:\\Users\\twilight\\Desktop\\ff1aff7b0ea667bbca02ebfbe00021bd213d5d0d.jpg";

    /**
     * 把一张图片转化成字节数组
     */
    public static byte[] image2Bytes(String path) {
        try {
            return Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0]; // 出错时返回空数组，或者可以返回 null
        }
    }

}

