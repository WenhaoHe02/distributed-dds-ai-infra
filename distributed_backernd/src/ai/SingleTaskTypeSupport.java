package ai;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class SingleTaskTypeSupport extends TypeSupport {
    private String type_name = "SingleTask";
    private static TypeCodeImpl s_typeCode = null;
    private static SingleTaskTypeSupport m_instance = new SingleTaskTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private SingleTaskTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        SingleTask sample = new SingleTask();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        SingleTask SingleTaskDst = (SingleTask)dst;
        SingleTask SingleTaskSrc = (SingleTask)src;
        SingleTaskDst.copy(SingleTaskSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        SingleTask sample = (SingleTask)_sample;
        int input_blobTmpLen = sample.input_blob.length();
        System.out.println("sample.input_blob.length():" +input_blobTmpLen);
        for (int i = 0; i < input_blobTmpLen; ++i){
            System.out.println("sample.input_blob.get_at(" + i + "):" + sample.input_blob.get_at(i));
        }
        if (sample.request_id != null){
            System.out.println("sample.request_id:" + sample.request_id);
        }
        else{
            System.out.println("sample.request_id: null");
        }
        if (sample.model_id != null){
            System.out.println("sample.model_id:" + sample.model_id);
        }
        else{
            System.out.println("sample.model_id: null");
        }
        if (sample.client_id != null){
            System.out.println("sample.client_id:" + sample.client_id);
        }
        else{
            System.out.println("sample.client_id: null");
        }
        if (sample.task_id != null){
            System.out.println("sample.task_id:" + sample.task_id);
        }
        else{
            System.out.println("sample.task_id: null");
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 1300;
    }

    public int get_max_key_sizeI(){
        return 1300;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new SingleTaskDataReader();}

    public DataWriter create_data_writer() {return new SingleTaskDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        SingleTask sample = (SingleTask)_sample;
        offset += CDRSerializer.get_untype_size(4, offset);
        int input_blobLen = sample.input_blob.length();
        if (input_blobLen != 0){
            offset += 1 * input_blobLen;
        }

        offset += CDRSerializer.get_string_size(sample.request_id == null ? 0 : sample.request_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.model_id == null ? 0 : sample.model_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.client_id == null ? 0 : sample.client_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.task_id == null ? 0 : sample.task_id.getBytes().length, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         SingleTask sample = (SingleTask) _sample;

        if (!CDRSerializer.put_int(cdr, sample.input_blob.length())){
            System.out.println("serialize length of sample.input_blob failed.");
            return -2;
        }
        if (sample.input_blob.length() != 0){
            if (!CDRSerializer.put_byte_array(cdr, sample.input_blob.get_contiguous_buffer(), sample.input_blob.length())){
                System.out.println("serialize sample.input_blob failed.");
                return -2;
            }
        }

        if (!CDRSerializer.put_string(cdr, sample.request_id, sample.request_id == null ? 0 : sample.request_id.length())){
            System.out.println("serialize sample.request_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.model_id, sample.model_id == null ? 0 : sample.model_id.length())){
            System.out.println("serialize sample.model_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.client_id, sample.client_id == null ? 0 : sample.client_id.length())){
            System.out.println("serialize sample.client_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.task_id, sample.task_id == null ? 0 : sample.task_id.length())){
            System.out.println("serialize sample.task_id failed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        SingleTask sample = (SingleTask) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.input_blob failed.");
            return -2;
        }
        if (!sample.input_blob.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.input_blob failed.");
            return -3;
        }
        if (!CDRDeserializer.get_byte_array(cdr, sample.input_blob.get_contiguous_buffer(), sample.input_blob.length())){
            System.out.println("deserialize sample.input_blob failed.");
            return -2;
        }

        sample.request_id = CDRDeserializer.get_string(cdr);
        if(sample.request_id ==null){
            System.out.println("deserialize member sample.request_id failed.");
            return -3;
        }

        sample.model_id = CDRDeserializer.get_string(cdr);
        if(sample.model_id ==null){
            System.out.println("deserialize member sample.model_id failed.");
            return -3;
        }

        sample.client_id = CDRDeserializer.get_string(cdr);
        if(sample.client_id ==null){
            System.out.println("deserialize member sample.client_id failed.");
            return -3;
        }

        sample.task_id = CDRDeserializer.get_string(cdr);
        if(sample.task_id ==null){
            System.out.println("deserialize member sample.task_id failed.");
            return -3;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        SingleTask sample = (SingleTask)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        SingleTask sample = (SingleTask)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        SingleTask sample = (SingleTask)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("ai.SingleTask");
        if (s_typeCode == null){
            System.out.println("create struct SingleTask typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UCHAR);
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member input_blob TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "input_blob",
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
            System.out.println("Get Member request_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
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
            System.out.println("Get Member model_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
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
            System.out.println("Get Member client_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
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

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member task_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            4,
            4,
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

        return s_typeCode;
    }

}