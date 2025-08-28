package com.example.ocrclient.data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class ResultItemTypeSupport extends TypeSupport {
    private String type_name = "ResultItem";
    private static TypeCodeImpl s_typeCode = null;
    private static ResultItemTypeSupport m_instance = new ResultItemTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private ResultItemTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        ResultItem sample = new ResultItem();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        ResultItem ResultItemDst = (ResultItem)dst;
        ResultItem ResultItemSrc = (ResultItem)src;
        ResultItemDst.copy(ResultItemSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        ResultItem sample = (ResultItem)_sample;
        if (sample.task_id != null){
            System.out.println("sample.task_id:" + sample.task_id);
        }
        else{
            System.out.println("sample.task_id: null");
        }
        if (sample.status != null){
            System.out.println("sample.status:" + sample.status);
        }
        else{
            System.out.println("sample.status: null");
        }
        int output_blobTmpLen = sample.output_blob.length();
        System.out.println("sample.output_blob.length():" +output_blobTmpLen);
        for (int i = 0; i < output_blobTmpLen; ++i){
            System.out.println("sample.output_blob.get_at(" + i + "):" + sample.output_blob.get_at(i));
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 779;
    }

    public int get_max_key_sizeI(){
        return 779;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new ResultItemDataReader();}

    public DataWriter create_data_writer() {return new ResultItemDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        ResultItem sample = (ResultItem)_sample;
        offset += CDRSerializer.get_string_size(sample.task_id == null ? 0 : sample.task_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.status == null ? 0 : sample.status.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int output_blobLen = sample.output_blob.length();
        if (output_blobLen != 0){
            offset += 1 * output_blobLen;
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         ResultItem sample = (ResultItem) _sample;

        if (!CDRSerializer.put_string(cdr, sample.task_id, sample.task_id == null ? 0 : sample.task_id.length())){
            System.out.println("serialize sample.task_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.status, sample.status == null ? 0 : sample.status.length())){
            System.out.println("serialize sample.status failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.output_blob.length())){
            System.out.println("serialize length of sample.output_blob failed.");
            return -2;
        }
        if (sample.output_blob.length() != 0){
            if (!CDRSerializer.put_byte_array(cdr, sample.output_blob.get_contiguous_buffer(), sample.output_blob.length())){
                System.out.println("serialize sample.output_blob failed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        ResultItem sample = (ResultItem) _sample;
        sample.task_id = CDRDeserializer.get_string(cdr);
        if(sample.task_id ==null){
            System.out.println("deserialize member sample.task_id failed.");
            return -3;
        }

        sample.status = CDRDeserializer.get_string(cdr);
        if(sample.status ==null){
            System.out.println("deserialize member sample.status failed.");
            return -3;
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.output_blob failed.");
            return -2;
        }
        if (!sample.output_blob.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.output_blob failed.");
            return -3;
        }
        if (!CDRDeserializer.get_byte_array(cdr, sample.output_blob.get_contiguous_buffer(), sample.output_blob.length())){
            System.out.println("deserialize sample.output_blob failed.");
            return -2;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        ResultItem sample = (ResultItem)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        ResultItem sample = (ResultItem)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        ResultItem sample = (ResultItem)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("ResultItem");
        if (s_typeCode == null){
            System.out.println("create struct ResultItem typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.create_string_TC(255);
        if (memberTc == null){
            System.out.println("Get Member task_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
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
            System.out.println("Get Member status TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
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

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UCHAR);
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member output_blob TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
            "output_blob",
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