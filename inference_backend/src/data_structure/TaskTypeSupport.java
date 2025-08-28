package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class TaskTypeSupport extends TypeSupport {
    private String type_name = "Task";
    private static TypeCodeImpl s_typeCode = null;
    private static TaskTypeSupport m_instance = new TaskTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private TaskTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        Task sample = new Task();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        Task TaskDst = (Task)dst;
        Task TaskSrc = (Task)src;
        TaskDst.copy(TaskSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        Task sample = (Task)_sample;
        if (sample.request_id != null){
            System.out.println("sample.request_id:" + sample.request_id);
        }
        else{
            System.out.println("sample.request_id: null");
        }
        if (sample.task_id != null){
            System.out.println("sample.task_id:" + sample.task_id);
        }
        else{
            System.out.println("sample.task_id: null");
        }
        if (sample.client_id != null){
            System.out.println("sample.client_id:" + sample.client_id);
        }
        else{
            System.out.println("sample.client_id: null");
        }
        int payloadTmpLen = sample.payload.length();
        System.out.println("sample.payload.length():" +payloadTmpLen);
        for (int i = 0; i < payloadTmpLen; ++i){
            System.out.println("sample.payload.get_at(" + i + "):" + sample.payload.get_at(i));
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 1039;
    }

    public int get_max_key_sizeI(){
        return 1039;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new TaskDataReader();}

    public DataWriter create_data_writer() {return new TaskDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        Task sample = (Task)_sample;
        offset += CDRSerializer.get_string_size(sample.request_id == null ? 0 : sample.request_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.task_id == null ? 0 : sample.task_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.client_id == null ? 0 : sample.client_id.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int payloadLen = sample.payload.length();
        if (payloadLen != 0){
            offset += 1 * payloadLen;
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         Task sample = (Task) _sample;

        if (!CDRSerializer.put_string(cdr, sample.request_id, sample.request_id == null ? 0 : sample.request_id.length())){
            System.out.println("serialize sample.request_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.task_id, sample.task_id == null ? 0 : sample.task_id.length())){
            System.out.println("serialize sample.task_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.client_id, sample.client_id == null ? 0 : sample.client_id.length())){
            System.out.println("serialize sample.client_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.payload.length())){
            System.out.println("serialize length of sample.payload failed.");
            return -2;
        }
        if (sample.payload.length() != 0){
            if (!CDRSerializer.put_byte_array(cdr, sample.payload.get_contiguous_buffer(), sample.payload.length())){
                System.out.println("serialize sample.payload failed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        Task sample = (Task) _sample;
        sample.request_id = CDRDeserializer.get_string(cdr);
        if(sample.request_id ==null){
            System.out.println("deserialize member sample.request_id failed.");
            return -3;
        }

        sample.task_id = CDRDeserializer.get_string(cdr);
        if(sample.task_id ==null){
            System.out.println("deserialize member sample.task_id failed.");
            return -3;
        }

        sample.client_id = CDRDeserializer.get_string(cdr);
        if(sample.client_id ==null){
            System.out.println("deserialize member sample.client_id failed.");
            return -3;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.payload failed.");
            return -2;
        }
        if (!sample.payload.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.payload failed.");
            return -3;
        }
        if (!CDRDeserializer.get_byte_array(cdr, sample.payload.get_contiguous_buffer(), sample.payload.length())){
            System.out.println("deserialize sample.payload failed.");
            return -2;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        Task sample = (Task)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        Task sample = (Task)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        Task sample = (Task)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.Task");
        if (s_typeCode == null){
            System.out.println("create struct Task typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member request_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "request_id",
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
            System.out.println("Get Member task_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "task_id",
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
            System.out.println("Get Member client_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
            "client_id",
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

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UCHAR);
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member payload TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
            "payload",
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

        return s_typeCode;
    }

}