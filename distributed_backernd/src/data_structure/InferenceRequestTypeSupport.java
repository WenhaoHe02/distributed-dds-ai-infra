package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class InferenceRequestTypeSupport extends TypeSupport {
    private String type_name = "InferenceRequest";
    private static TypeCodeImpl s_typeCode = null;
    private static InferenceRequestTypeSupport m_instance = new InferenceRequestTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private InferenceRequestTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        InferenceRequest sample = new InferenceRequest();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        InferenceRequest InferenceRequestDst = (InferenceRequest)dst;
        InferenceRequest InferenceRequestSrc = (InferenceRequest)src;
        InferenceRequestDst.copy(InferenceRequestSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        InferenceRequest sample = (InferenceRequest)_sample;
        if (sample.request_id != null){
            System.out.println("sample.request_id:" + sample.request_id);
        }
        else{
            System.out.println("sample.request_id: null");
        }
        int tasksTmpLen = sample.tasks.length();
        System.out.println("sample.tasks.length():" +tasksTmpLen);
        for (int i = 0; i < tasksTmpLen; ++i){
            data_structure.SingleTaskTypeSupport.get_instance().print_sample(sample.tasks.get_at(i));
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 331763;
    }

    public int get_max_key_sizeI(){
        return 331763;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new InferenceRequestDataReader();}

    public DataWriter create_data_writer() {return new InferenceRequestDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        InferenceRequest sample = (InferenceRequest)_sample;
        offset += CDRSerializer.get_string_size(sample.request_id == null ? 0 : sample.request_id.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int tasksLen = sample.tasks.length();
        if (tasksLen != 0){
            for (int i = 0; i < tasksLen; ++i){
                data_structure.SingleTask curEle = sample.tasks.get_at(i);
                offset += data_structure.SingleTaskTypeSupport.get_instance().get_sizeI(curEle, cdr, offset);
            }
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         InferenceRequest sample = (InferenceRequest) _sample;

        if (!CDRSerializer.put_string(cdr, sample.request_id, sample.request_id == null ? 0 : sample.request_id.length())){
            System.out.println("serialize sample.request_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.tasks.length())){
            System.out.println("serialize length of sample.tasks failed.");
            return -2;
        }
        for (int i = 0; i < sample.tasks.length(); ++i){
            if (data_structure.SingleTaskTypeSupport.get_instance().serializeI(sample.tasks.get_at(i),cdr) < 0){
                System.out.println("serialize sample.tasksfailed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        InferenceRequest sample = (InferenceRequest) _sample;
        sample.request_id = CDRDeserializer.get_string(cdr);
        if(sample.request_id ==null){
            System.out.println("deserialize member sample.request_id failed.");
            return -3;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.tasks failed.");
            return -2;
        }
        if (!sample.tasks.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.tasks failed.");
            return -3;
        }
        data_structure.SingleTask tmptasks = new data_structure.SingleTask();
        for (int i = 0; i < sample.tasks.length(); ++i){
            if (data_structure.SingleTaskTypeSupport.get_instance().deserializeI(tmptasks, cdr) < 0){
                System.out.println("deserialize sample.tasks failed.");
                return -2;
            }
            sample.tasks.set_at(i, tmptasks);
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        InferenceRequest sample = (InferenceRequest)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        InferenceRequest sample = (InferenceRequest)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        InferenceRequest sample = (InferenceRequest)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.InferenceRequest");
        if (s_typeCode == null){
            System.out.println("create struct InferenceRequest typecode failed.");
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

        memberTc = (TypeCodeImpl)data_structure.SingleTaskTypeSupport.get_instance().get_typecode();
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member tasks TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "tasks",
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