package ai;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class AggregatedResultTypeSupport extends TypeSupport {
    private String type_name = "AggregatedResult";
    private static TypeCodeImpl s_typeCode = null;
    private static AggregatedResultTypeSupport m_instance = new AggregatedResultTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private AggregatedResultTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        AggregatedResult sample = new AggregatedResult();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        AggregatedResult AggregatedResultDst = (AggregatedResult)dst;
        AggregatedResult AggregatedResultSrc = (AggregatedResult)src;
        AggregatedResultDst.copy(AggregatedResultSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        AggregatedResult sample = (AggregatedResult)_sample;
        int resultsTmpLen = sample.results.length();
        System.out.println("sample.results.length():" +resultsTmpLen);
        for (int i = 0; i < resultsTmpLen; ++i){
            ai.SingleResultTypeSupport.get_instance().print_sample(sample.results.get_at(i));
        }
        if (sample.client_id != null){
            System.out.println("sample.client_id:" + sample.client_id);
        }
        else{
            System.out.println("sample.client_id: null");
        }
        if (sample.request_id != null){
            System.out.println("sample.request_id:" + sample.request_id);
        }
        else{
            System.out.println("sample.request_id: null");
        }
        if (sample.status != null){
            System.out.println("sample.status:" + sample.status);
        }
        else{
            System.out.println("sample.status: null");
        }
        if (sample.error_message != null){
            System.out.println("sample.error_message:" + sample.error_message);
        }
        else{
            System.out.println("sample.error_message: null");
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 532464;
    }

    public int get_max_key_sizeI(){
        return 532464;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new AggregatedResultDataReader();}

    public DataWriter create_data_writer() {return new AggregatedResultDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        AggregatedResult sample = (AggregatedResult)_sample;
        offset += CDRSerializer.get_untype_size(4, offset);
        int resultsLen = sample.results.length();
        if (resultsLen != 0){
            for (int i = 0; i < resultsLen; ++i){
                ai.SingleResult curEle = sample.results.get_at(i);
                offset += ai.SingleResultTypeSupport.get_instance().get_sizeI(curEle, cdr, offset);
            }
        }

        offset += CDRSerializer.get_string_size(sample.client_id == null ? 0 : sample.client_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.request_id == null ? 0 : sample.request_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.status == null ? 0 : sample.status.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.error_message == null ? 0 : sample.error_message.getBytes().length, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         AggregatedResult sample = (AggregatedResult) _sample;

        if (!CDRSerializer.put_int(cdr, sample.results.length())){
            System.out.println("serialize length of sample.results failed.");
            return -2;
        }
        for (int i = 0; i < sample.results.length(); ++i){
            if (ai.SingleResultTypeSupport.get_instance().serializeI(sample.results.get_at(i),cdr) < 0){
                System.out.println("serialize sample.resultsfailed.");
                return -2;
            }
        }

        if (!CDRSerializer.put_string(cdr, sample.client_id, sample.client_id == null ? 0 : sample.client_id.length())){
            System.out.println("serialize sample.client_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.request_id, sample.request_id == null ? 0 : sample.request_id.length())){
            System.out.println("serialize sample.request_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.status, sample.status == null ? 0 : sample.status.length())){
            System.out.println("serialize sample.status failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.error_message, sample.error_message == null ? 0 : sample.error_message.length())){
            System.out.println("serialize sample.error_message failed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        AggregatedResult sample = (AggregatedResult) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.results failed.");
            return -2;
        }
        if (!sample.results.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.results failed.");
            return -3;
        }
        ai.SingleResult tmpresults = new ai.SingleResult();
        for (int i = 0; i < sample.results.length(); ++i){
            if (ai.SingleResultTypeSupport.get_instance().deserializeI(tmpresults, cdr) < 0){
                System.out.println("deserialize sample.results failed.");
                return -2;
            }
            sample.results.set_at(i, tmpresults);
        }

        sample.client_id = CDRDeserializer.get_string(cdr);
        if(sample.client_id ==null){
            System.out.println("deserialize member sample.client_id failed.");
            return -3;
        }

        sample.request_id = CDRDeserializer.get_string(cdr);
        if(sample.request_id ==null){
            System.out.println("deserialize member sample.request_id failed.");
            return -3;
        }

        sample.status = CDRDeserializer.get_string(cdr);
        if(sample.status ==null){
            System.out.println("deserialize member sample.status failed.");
            return -3;
        }

        sample.error_message = CDRDeserializer.get_string(cdr);
        if(sample.error_message ==null){
            System.out.println("deserialize member sample.error_message failed.");
            return -3;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        AggregatedResult sample = (AggregatedResult)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        AggregatedResult sample = (AggregatedResult)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        AggregatedResult sample = (AggregatedResult)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("ai.AggregatedResult");
        if (s_typeCode == null){
            System.out.println("create struct AggregatedResult typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = (TypeCodeImpl)ai.SingleResultTypeSupport.get_instance().get_typecode();
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member results TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
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

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member client_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
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
            System.out.println("Get Member request_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
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

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member error_message TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            4,
            4,
            "error_message",
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