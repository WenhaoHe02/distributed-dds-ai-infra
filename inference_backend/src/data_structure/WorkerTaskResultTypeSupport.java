package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class WorkerTaskResultTypeSupport extends TypeSupport {
    private String type_name = "WorkerTaskResult";
    private static TypeCodeImpl s_typeCode = null;
    private static WorkerTaskResultTypeSupport m_instance = new WorkerTaskResultTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private WorkerTaskResultTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        WorkerTaskResult sample = new WorkerTaskResult();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        WorkerTaskResult WorkerTaskResultDst = (WorkerTaskResult)dst;
        WorkerTaskResult WorkerTaskResultSrc = (WorkerTaskResult)src;
        WorkerTaskResultDst.copy(WorkerTaskResultSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        WorkerTaskResult sample = (WorkerTaskResult)_sample;
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
        if (sample.status != null){
            System.out.println("sample.status:" + sample.status);
        }
        else{
            System.out.println("sample.status: null");
        }
        data_structure.BytesTypeSupport.get_instance().print_sample(sample.output_blob);
        int textsTmpLen = sample.texts.length();
        System.out.println("sample.texts.length():" +textsTmpLen);
        for (int i = 0; i < textsTmpLen; ++i){
            if (sample.texts.get_at(i) != null){
                System.out.println("sample.texts.get_at(" + i + "):" + sample.texts.get_at(i));
            }
            else{
                System.out.println("sample.texts.get_at(" + i + "): null");
            }
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 0xffffffff;
    }

    public int get_max_key_sizeI(){
        return 0xffffffff;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new WorkerTaskResultDataReader();}

    public DataWriter create_data_writer() {return new WorkerTaskResultDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        WorkerTaskResult sample = (WorkerTaskResult)_sample;
        offset += CDRSerializer.get_string_size(sample.request_id == null ? 0 : sample.request_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.task_id == null ? 0 : sample.task_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.client_id == null ? 0 : sample.client_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.status == null ? 0 : sample.status.getBytes().length, offset);

        offset += data_structure.BytesTypeSupport.get_instance().get_sizeI(sample.output_blob, cdr, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int textsLen = sample.texts.length();
        if (textsLen != 0){
            for(int i = 0; i<sample.texts.length(); ++i)
            {
                offset += CDRSerializer.get_string_size(sample.texts.get_at(i).getBytes().length,offset);
            }
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         WorkerTaskResult sample = (WorkerTaskResult) _sample;

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

        if (!CDRSerializer.put_string(cdr, sample.status, sample.status == null ? 0 : sample.status.length())){
            System.out.println("serialize sample.status failed.");
            return -2;
        }

        if (data_structure.BytesTypeSupport.get_instance().serializeI(sample.output_blob,cdr) < 0){
            System.out.println("serialize sample.output_blobfailed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.texts.length())){
            System.out.println("serialize length of sample.texts failed.");
            return -2;
        }
        for (int i = 0; i < sample.texts.length(); ++i){
            if (!CDRSerializer.put_string(cdr, sample.texts.get_at(i), sample.texts.get_at(i) == null ? 0 : sample.texts.get_at(i).length())){
                System.out.println("serialize sample.texts failed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        WorkerTaskResult sample = (WorkerTaskResult) _sample;
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

        sample.status = CDRDeserializer.get_string(cdr);
        if(sample.status ==null){
            System.out.println("deserialize member sample.status failed.");
            return -3;
        }

        if (data_structure.BytesTypeSupport.get_instance().deserializeI(sample.output_blob, cdr) < 0){
            System.out.println("deserialize sample.output_blob failed.");
            return -2;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.texts failed.");
            return -2;
        }
        if (!sample.texts.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.texts failed.");
            return -3;
        }
        for(int i =0 ;i < sample.texts.length() ;++i)
        {
            sample.texts.set_at(i, CDRDeserializer.get_string(cdr));
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        WorkerTaskResult sample = (WorkerTaskResult)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        WorkerTaskResult sample = (WorkerTaskResult)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        WorkerTaskResult sample = (WorkerTaskResult)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.WorkerTaskResult");
        if (s_typeCode == null){
            System.out.println("create struct WorkerTaskResult typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.create_string_TC(0xffffffff);
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

        memberTc = factory.create_string_TC(0xffffffff);
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

        memberTc = factory.create_string_TC(0xffffffff);
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

        memberTc = factory.create_string_TC(0xffffffff);
        if (memberTc == null){
            System.out.println("Get Member status TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
            "status",
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

        memberTc = (TypeCodeImpl)data_structure.BytesTypeSupport.get_instance().get_typecode();
        if (memberTc == null){
            System.out.println("Get Member output_blob TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            4,
            4,
            "output_blob",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.create_string_TC(0xffffffff);
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(0xffffffff, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member texts TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            5,
            5,
            "texts",
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