package ai_train;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class ModelBlobTypeSupport extends TypeSupport {
    private String type_name = "ModelBlob";
    private static TypeCodeImpl s_typeCode = null;
    private static ModelBlobTypeSupport m_instance = new ModelBlobTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private ModelBlobTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        ModelBlob sample = new ModelBlob();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        ModelBlob ModelBlobDst = (ModelBlob)dst;
        ModelBlob ModelBlobSrc = (ModelBlob)src;
        ModelBlobDst.copy(ModelBlobSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        ModelBlob sample = (ModelBlob)_sample;
        System.out.println("sample.round_id:" + sample.round_id);
        int dataTmpLen = sample.data.length();
        System.out.println("sample.data.length():" +dataTmpLen);
        for (int i = 0; i < dataTmpLen; ++i){
            System.out.println("sample.data.get_at(" + i + "):" + sample.data.get_at(i));
        }
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 263;
    }

    public int get_max_key_sizeI(){
        return 263;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new ModelBlobDataReader();}

    public DataWriter create_data_writer() {return new ModelBlobDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        ModelBlob sample = (ModelBlob)_sample;
        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int dataLen = sample.data.length();
        if (dataLen != 0){
            offset += 1 * dataLen;
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         ModelBlob sample = (ModelBlob) _sample;

        if (!CDRSerializer.put_int(cdr, sample.round_id)){
            System.out.println("serialize sample.round_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.data.length())){
            System.out.println("serialize length of sample.data failed.");
            return -2;
        }
        if (sample.data.length() != 0){
            if (!CDRSerializer.put_byte_array(cdr, sample.data.get_contiguous_buffer(), sample.data.length())){
                System.out.println("serialize sample.data failed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        ModelBlob sample = (ModelBlob) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.round_id failed.");
            return -2;
        }
        sample.round_id= tmp_int_obj[0];

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.data failed.");
            return -2;
        }
        if (!sample.data.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.data failed.");
            return -3;
        }
        if (!CDRDeserializer.get_byte_array(cdr, sample.data.get_contiguous_buffer(), sample.data.length())){
            System.out.println("deserialize sample.data failed.");
            return -2;
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        ModelBlob sample = (ModelBlob)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        ModelBlob sample = (ModelBlob)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        ModelBlob sample = (ModelBlob)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("ai_train.ModelBlob");
        if (s_typeCode == null){
            System.out.println("create struct ModelBlob typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UINT);
        if (memberTc == null){
            System.out.println("Get Member round_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
            "round_id",
            memberTc,
            false,
            false);
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
            System.out.println("Get Member data TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "data",
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