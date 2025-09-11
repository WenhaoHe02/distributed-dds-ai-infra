package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class GrantTypeSupport extends TypeSupport {
    private String type_name = "Grant";
    private static TypeCodeImpl s_typeCode = null;
    private static GrantTypeSupport m_instance = new GrantTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private GrantTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        Grant sample = new Grant();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        Grant GrantDst = (Grant)dst;
        Grant GrantSrc = (Grant)src;
        GrantDst.copy(GrantSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        Grant sample = (Grant)_sample;
        if (sample.batch_id != null){
            System.out.println("sample.batch_id:" + sample.batch_id);
        }
        else{
            System.out.println("sample.batch_id: null");
        }
        if (sample.winner_worker_id != null){
            System.out.println("sample.winner_worker_id:" + sample.winner_worker_id);
        }
        else{
            System.out.println("sample.winner_worker_id: null");
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

    public DataReader create_data_reader() {return new GrantDataReader();}

    public DataWriter create_data_writer() {return new GrantDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        Grant sample = (Grant)_sample;
        offset += CDRSerializer.get_string_size(sample.batch_id == null ? 0 : sample.batch_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.winner_worker_id == null ? 0 : sample.winner_worker_id.getBytes().length, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         Grant sample = (Grant) _sample;

        if (!CDRSerializer.put_string(cdr, sample.batch_id, sample.batch_id == null ? 0 : sample.batch_id.length())){
            System.out.println("serialize sample.batch_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.winner_worker_id, sample.winner_worker_id == null ? 0 : sample.winner_worker_id.length())){
            System.out.println("serialize sample.winner_worker_id failed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        Grant sample = (Grant) _sample;
        sample.batch_id = CDRDeserializer.get_string(cdr);
        if(sample.batch_id ==null){
            System.out.println("deserialize member sample.batch_id failed.");
            return -3;
        }

        sample.winner_worker_id = CDRDeserializer.get_string(cdr);
        if(sample.winner_worker_id ==null){
            System.out.println("deserialize member sample.winner_worker_id failed.");
            return -3;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        Grant sample = (Grant)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        Grant sample = (Grant)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        Grant sample = (Grant)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.Grant");
        if (s_typeCode == null){
            System.out.println("create struct Grant typecode failed.");
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
            System.out.println("Get Member winner_worker_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "winner_worker_id",
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