package com.example.ocrclient.ai;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class KVListTypeSupport extends TypeSupport {
    private String type_name = "KVList";
    private static TypeCodeImpl s_typeCode = null;
    private static KVListTypeSupport m_instance = new KVListTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private KVListTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        KVList sample = new KVList();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        KVList KVListDst = (KVList)dst;
        KVList KVListSrc = (KVList)src;
        KVListDst.copy(KVListSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        KVList sample = (KVList)_sample;
        int valueTmpLen = sample.value.length();
        System.out.println("sample.value.length():" +valueTmpLen);
        for (int i = 0; i < valueTmpLen; ++i){
            KVTypeSupport.get_instance().print_sample(sample.value.get_at(i));
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 132604;
    }

    public int get_max_key_sizeI(){
        return 132604;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new KVListDataReader();}

    public DataWriter create_data_writer() {return new KVListDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        KVList sample = (KVList)_sample;
        offset += CDRSerializer.get_untype_size(4, offset);
        int valueLen = sample.value.length();
        if (valueLen != 0){
            for (int i = 0; i < valueLen; ++i){
                KV curEle = sample.value.get_at(i);
                offset += KVTypeSupport.get_instance().get_sizeI(curEle, cdr, offset);
            }
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         KVList sample = (KVList) _sample;

        if (!CDRSerializer.put_int(cdr, sample.value.length())){
            System.out.println("serialize length of sample.value failed.");
            return -2;
        }
        for (int i = 0; i < sample.value.length(); ++i){
            if (KVTypeSupport.get_instance().serializeI(sample.value.get_at(i),cdr) < 0){
                System.out.println("serialize sample.valuefailed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        KVList sample = (KVList) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.value failed.");
            return -2;
        }
        if (!sample.value.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.value failed.");
            return -3;
        }
        KV tmpvalue = new KV();
        for (int i = 0; i < sample.value.length(); ++i){
            if (KVTypeSupport.get_instance().deserializeI(tmpvalue, cdr) < 0){
                System.out.println("deserialize sample.value failed.");
                return -2;
            }
            sample.value.set_at(i, tmpvalue);
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        KVList sample = (KVList)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        KVList sample = (KVList)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        KVList sample = (KVList)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("KVList");
        if (s_typeCode == null){
            System.out.println("create struct KVList typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = (TypeCodeImpl)KVTypeSupport.get_instance().get_typecode();
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
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