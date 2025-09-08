package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class WorkerResultTypeSupport extends TypeSupport {
    private String type_name = "WorkerResult";
    private static TypeCodeImpl s_typeCode = null;
    private static WorkerResultTypeSupport m_instance = new WorkerResultTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private WorkerResultTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        WorkerResult sample = new WorkerResult();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        WorkerResult WorkerResultDst = (WorkerResult)dst;
        WorkerResult WorkerResultSrc = (WorkerResult)src;
        WorkerResultDst.copy(WorkerResultSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        WorkerResult sample = (WorkerResult)_sample;
        if (sample.batch_id != null){
            System.out.println("sample.batch_id:" + sample.batch_id);
        }
        else{
            System.out.println("sample.batch_id: null");
        }
        if (sample.model_id != null){
            System.out.println("sample.model_id:" + sample.model_id);
        }
        else{
            System.out.println("sample.model_id: null");
        }
        if (sample.worker_id != null){
            System.out.println("sample.worker_id:" + sample.worker_id);
        }
        else{
            System.out.println("sample.worker_id: null");
        }
        int resultsTmpLen = sample.results.length();
        System.out.println("sample.results.length():" +resultsTmpLen);
        for (int i = 0; i < resultsTmpLen; ++i){
            data_structure.WorkerTaskResultTypeSupport.get_instance().print_sample(sample.results.get_at(i));
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

    public DataReader create_data_reader() {return new WorkerResultDataReader();}

    public DataWriter create_data_writer() {return new WorkerResultDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        WorkerResult sample = (WorkerResult)_sample;
        offset += CDRSerializer.get_string_size(sample.batch_id == null ? 0 : sample.batch_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.model_id == null ? 0 : sample.model_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.worker_id == null ? 0 : sample.worker_id.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int resultsLen = sample.results.length();
        if (resultsLen != 0){
            for (int i = 0; i < resultsLen; ++i){
                data_structure.WorkerTaskResult curEle = sample.results.get_at(i);
                offset += data_structure.WorkerTaskResultTypeSupport.get_instance().get_sizeI(curEle, cdr, offset);
            }
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         WorkerResult sample = (WorkerResult) _sample;

        if (!CDRSerializer.put_string(cdr, sample.batch_id, sample.batch_id == null ? 0 : sample.batch_id.length())){
            System.out.println("serialize sample.batch_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.model_id, sample.model_id == null ? 0 : sample.model_id.length())){
            System.out.println("serialize sample.model_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.worker_id, sample.worker_id == null ? 0 : sample.worker_id.length())){
            System.out.println("serialize sample.worker_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.results.length())){
            System.out.println("serialize length of sample.results failed.");
            return -2;
        }
        for (int i = 0; i < sample.results.length(); ++i){
            if (data_structure.WorkerTaskResultTypeSupport.get_instance().serializeI(sample.results.get_at(i),cdr) < 0){
                System.out.println("serialize sample.resultsfailed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        WorkerResult sample = (WorkerResult) _sample;
        sample.batch_id = CDRDeserializer.get_string(cdr);
        if(sample.batch_id ==null){
            System.out.println("deserialize member sample.batch_id failed.");
            return -3;
        }

        sample.model_id = CDRDeserializer.get_string(cdr);
        if(sample.model_id ==null){
            System.out.println("deserialize member sample.model_id failed.");
            return -3;
        }

        sample.worker_id = CDRDeserializer.get_string(cdr);
        if(sample.worker_id ==null){
            System.out.println("deserialize member sample.worker_id failed.");
            return -3;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.results failed.");
            return -2;
        }
        if (!sample.results.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.results failed.");
            return -3;
        }
        data_structure.WorkerTaskResult tmpresults = new data_structure.WorkerTaskResult();
        for (int i = 0; i < sample.results.length(); ++i){
            if (data_structure.WorkerTaskResultTypeSupport.get_instance().deserializeI(tmpresults, cdr) < 0){
                System.out.println("deserialize sample.results failed.");
                return -2;
            }
            sample.results.set_at(i, tmpresults);
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        WorkerResult sample = (WorkerResult)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        WorkerResult sample = (WorkerResult)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        WorkerResult sample = (WorkerResult)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.WorkerResult");
        if (s_typeCode == null){
            System.out.println("create struct WorkerResult typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.create_string_TC(0xffffffff);
        if (memberTc == null){
            System.out.println("Get Member batch_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "batch_id",
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

        memberTc = factory.create_string_TC(0xffffffff);
        if (memberTc == null){
            System.out.println("Get Member worker_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
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

        memberTc = (TypeCodeImpl)data_structure.WorkerTaskResultTypeSupport.get_instance().get_typecode();
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(0xffffffff, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member results TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
            "results",
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