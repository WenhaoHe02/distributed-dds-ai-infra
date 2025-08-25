package ai;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class NodeStatusTypeSupport extends TypeSupport {
    private String type_name = "NodeStatus";
    private static TypeCodeImpl s_typeCode = null;
    private static NodeStatusTypeSupport m_instance = new NodeStatusTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private NodeStatusTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        NodeStatus sample = new NodeStatus();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        NodeStatus NodeStatusDst = (NodeStatus)dst;
        NodeStatus NodeStatusSrc = (NodeStatus)src;
        NodeStatusDst.copy(NodeStatusSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        NodeStatus sample = (NodeStatus)_sample;
        if (sample.worker_id != null){
            System.out.println("sample.worker_id:" + sample.worker_id);
        }
        else{
            System.out.println("sample.worker_id: null");
        }
        if (sample.model_id != null){
            System.out.println("sample.model_id:" + sample.model_id);
        }
        else{
            System.out.println("sample.model_id: null");
        }
        if (sample.host != null){
            System.out.println("sample.host:" + sample.host);
        }
        else{
            System.out.println("sample.host: null");
        }
        System.out.println("sample.queue_depth:" + sample.queue_depth);
        System.out.println("sample.est_capacity:" + sample.est_capacity);
        if (sample.health != null){
            System.out.println("sample.health:" + sample.health);
        }
        else{
            System.out.println("sample.health: null");
        }
        System.out.println("sample.heartbeat_ms:" + sample.heartbeat_ms);
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 1052;
    }

    public int get_max_key_sizeI(){
        return 1052;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new NodeStatusDataReader();}

    public DataWriter create_data_writer() {return new NodeStatusDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        NodeStatus sample = (NodeStatus)_sample;
        offset += CDRSerializer.get_string_size(sample.worker_id == null ? 0 : sample.worker_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.model_id == null ? 0 : sample.model_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.host == null ? 0 : sample.host.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_string_size(sample.health == null ? 0 : sample.health.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         NodeStatus sample = (NodeStatus) _sample;

        if (!CDRSerializer.put_string(cdr, sample.worker_id, sample.worker_id == null ? 0 : sample.worker_id.length())){
            System.out.println("serialize sample.worker_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.model_id, sample.model_id == null ? 0 : sample.model_id.length())){
            System.out.println("serialize sample.model_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.host, sample.host == null ? 0 : sample.host.length())){
            System.out.println("serialize sample.host failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.queue_depth)){
            System.out.println("serialize sample.queue_depth failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.est_capacity)){
            System.out.println("serialize sample.est_capacity failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.health, sample.health == null ? 0 : sample.health.length())){
            System.out.println("serialize sample.health failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.heartbeat_ms)){
            System.out.println("serialize sample.heartbeat_ms failed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        NodeStatus sample = (NodeStatus) _sample;
        sample.worker_id = CDRDeserializer.get_string(cdr);
        if(sample.worker_id ==null){
            System.out.println("deserialize member sample.worker_id failed.");
            return -3;
        }

        sample.model_id = CDRDeserializer.get_string(cdr);
        if(sample.model_id ==null){
            System.out.println("deserialize member sample.model_id failed.");
            return -3;
        }

        sample.host = CDRDeserializer.get_string(cdr);
        if(sample.host ==null){
            System.out.println("deserialize member sample.host failed.");
            return -3;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.queue_depth failed.");
            return -2;
        }
        sample.queue_depth= tmp_int_obj[0];

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.est_capacity failed.");
            return -2;
        }
        sample.est_capacity= tmp_int_obj[0];

        sample.health = CDRDeserializer.get_string(cdr);
        if(sample.health ==null){
            System.out.println("deserialize member sample.health failed.");
            return -3;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.heartbeat_ms failed.");
            return -2;
        }
        sample.heartbeat_ms= tmp_int_obj[0];

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        NodeStatus sample = (NodeStatus)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        NodeStatus sample = (NodeStatus)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        NodeStatus sample = (NodeStatus)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("ai.NodeStatus");
        if (s_typeCode == null){
            System.out.println("create struct NodeStatus typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member worker_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "worker_id",
            memberTc,
            false,
            false);
        factory.delete_TC(memberTc);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member model_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "model_id",
            memberTc,
            false,
            false);
        factory.delete_TC(memberTc);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member host TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
            "host",
            memberTc,
            false,
            false);
        factory.delete_TC(memberTc);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_INT);
        if (memberTc == null){
            System.out.println("Get Member queue_depth TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
            "queue_depth",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_INT);
        if (memberTc == null){
            System.out.println("Get Member est_capacity TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            4,
            4,
            "est_capacity",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member health TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            5,
            5,
            "health",
            memberTc,
            false,
            false);
        factory.delete_TC(memberTc);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_INT);
        if (memberTc == null){
            System.out.println("Get Member heartbeat_ms TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            6,
            6,
            "heartbeat_ms",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        return s_typeCode;
    }

}