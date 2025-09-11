package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class BytesTypeSupport extends TypeSupport {
    private String type_name = "Bytes";
    private static TypeCodeImpl s_typeCode = null;
    private static BytesTypeSupport m_instance = new BytesTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private BytesTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        Bytes sample = new Bytes();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        Bytes BytesDst = (Bytes)dst;
        Bytes BytesSrc = (Bytes)src;
        BytesDst.copy(BytesSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        Bytes sample = (Bytes)_sample;
        int valueTmpLen = sample.value.length();
        System.out.println("sample.value.length():" +valueTmpLen);
        for (int i = 0; i < valueTmpLen; ++i){
            System.out.println("sample.value.get_at(" + i + "):" + sample.value.get_at(i));
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

    public DataReader create_data_reader() {return new BytesDataReader();}

    public DataWriter create_data_writer() {return new BytesDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        Bytes sample = (Bytes)_sample;
        offset += CDRSerializer.get_untype_size(4, offset);
        int valueLen = sample.value.length();
        if (valueLen != 0){
            offset += 1 * valueLen;
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         Bytes sample = (Bytes) _sample;

        if (!CDRSerializer.put_int(cdr, sample.value.length())){
            System.out.println("serialize length of sample.value failed.");
            return -2;
        }
        if (sample.value.length() != 0){
            if (!CDRSerializer.put_byte_array(cdr, sample.value.get_contiguous_buffer(), sample.value.length())){
                System.out.println("serialize sample.value failed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        Bytes sample = (Bytes) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.value failed.");
            return -2;
        }
        if (!sample.value.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.value failed.");
            return -3;
        }
        if (!CDRDeserializer.get_byte_array(cdr, sample.value.get_contiguous_buffer(), sample.value.length())){
            System.out.println("deserialize sample.value failed.");
            return -2;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        Bytes sample = (Bytes)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        Bytes sample = (Bytes)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        Bytes sample = (Bytes)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.Bytes");
        if (s_typeCode == null){
            System.out.println("create struct Bytes typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UCHAR);
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(0xffffffff, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member value TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "value",
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