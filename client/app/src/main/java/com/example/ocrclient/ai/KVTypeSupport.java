package com.example.ocrclient.ai;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class KVTypeSupport extends TypeSupport {
    private String type_name = "KV";
    private static TypeCodeImpl s_typeCode = null;
    private static KVTypeSupport m_instance = new KVTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private KVTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        KV sample = new KV();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        KV KVDst = (KV)dst;
        KV KVSrc = (KV)src;
        KVDst.copy(KVSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        KV sample = (KV)_sample;
        if (sample.key != null){
            System.out.println("sample.key:" + sample.key);
        }
        else{
            System.out.println("sample.key: null");
        }
        if (sample.value != null){
            System.out.println("sample.value:" + sample.value);
        }
        else{
            System.out.println("sample.value: null");
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 520;
    }

    public int get_max_key_sizeI(){
        return 520;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new KVDataReader();}

    public DataWriter create_data_writer() {return new KVDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        KV sample = (KV)_sample;
        offset += CDRSerializer.get_string_size(sample.key == null ? 0 : sample.key.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.value == null ? 0 : sample.value.getBytes().length, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         KV sample = (KV) _sample;

        if (!CDRSerializer.put_string(cdr, sample.key, sample.key == null ? 0 : sample.key.length())){
            System.out.println("serialize sample.key failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.value, sample.value == null ? 0 : sample.value.length())){
            System.out.println("serialize sample.value failed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        KV sample = (KV) _sample;
        sample.key = CDRDeserializer.get_string(cdr);
        if(sample.key ==null){
            System.out.println("deserialize member sample.key failed.");
            return -3;
        }

        sample.value = CDRDeserializer.get_string(cdr);
        if(sample.value ==null){
            System.out.println("deserialize member sample.value failed.");
            return -3;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        KV sample = (KV)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        KV sample = (KV)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        KV sample = (KV)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("KV");
        if (s_typeCode == null){
            System.out.println("create struct KV typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member key TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "key",
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
            System.out.println("Get Member value TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
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