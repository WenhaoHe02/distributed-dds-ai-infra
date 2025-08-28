import com.zrdds.domain.DomainParticipant;
import com.zrdds.domain.DomainParticipantFactory;
import com.zrdds.infrastructure.*;
import com.zrdds.publication.DataWriter;
import com.zrdds.simpleinterface.DDSIF;

public class simpleInterfaceBytesTypePub {

    public static void main(String[] args) {
        // 初始化DDS
        DomainParticipantFactory factory = DDSIF.init("ZRDDS_QOS_PROFILES.xml", "non_rio");
        if (factory == null) {
            System.out.println("DDSIF.init failed.");
            return;
        }
        // 创建使用udp的域参与者
        DomainParticipant udpParticipant = DDSIF.create_dp(150, "udp_dp");
        if (udpParticipant == null) {
            System.out.println("create udp dp failed.");
            return;
        }
        // 发布AirTrack主题
        DataWriter writer = DDSIF.pub_topic(udpParticipant, "AirTrack", BytesTypeSupport.get_instance(), "non_zerocopy_reliable", null);
        BytesDataWriter bytesDataWriter = (BytesDataWriter)writer;
        if (bytesDataWriter == null) {
            System.out.println("pub AirTrack topic failed.");
            return;
        }

        // 发送数据
        int count = 0;
        while (count++ < 2000) {
            String content = "air track value " + count + " from java simpleInterfaceBytesTypePub.";
            byte[] buffer = content.getBytes();
            System.out.println("write sample " + content);
            // 第一种发送数据方法，直接指定DataWrtier的指针
            Bytes sample = new Bytes();
            DDSIF.BytesWrapper(sample, buffer, buffer.length);
            ReturnCode_t retCode = bytesDataWriter.write(sample, InstanceHandle_t.HANDLE_NIL_NATIVE);
            // 第二种发送数据的方法，指定域与主题名称
            //ReturnCode_t retCode = DDSIF.BytesWrite(150, "AirTrack", buffer, buffer.length);
            if (retCode != ReturnCode_t.RETCODE_OK) {
                System.out.println("DDSIF::BytesWrite failed.");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        DDSIF.Finalize();
    }
}
