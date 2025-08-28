package ai_train;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class TrainCmdTypeSupport extends TypeSupport {
    private String type_name = "TrainCmd";
    private static TypeCodeImpl s_typeCode = null;
    private static TrainCmdTypeSupport m_instance = new TrainCmdTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private TrainCmdTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        TrainCmd sample = new TrainCmd();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        TrainCmd TrainCmdDst = (TrainCmd)dst;
        TrainCmd TrainCmdSrc = (TrainCmd)src;
        TrainCmdDst.copy(TrainCmdSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        TrainCmd sample = (TrainCmd)_sample;
        System.out.println("sample.round_id:" + sample.round_id);
        System.out.println("sample.subset_size:" + sample.subset_size);
        System.out.println("sample.epochs:" + sample.epochs);
        System.out.println("sample.lr:" + sample.lr);
        System.out.println("sample.seed:" + sample.seed);
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 28;
    }

    public int get_max_key_sizeI(){
        return 28;
    }

    public boolean has_keyI(){
        return false;
    }

    public String get_keyhashI(Object sample, long cdr){
        return "-1";
    }

    public DataReader create_data_reader() {return new TrainCmdDataReader();}

    public DataWriter create_data_writer() {return new TrainCmdDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        TrainCmd sample = (TrainCmd)_sample;
        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_untype_size(8, offset);

        offset += CDRSerializer.get_untype_size(4, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         TrainCmd sample = (TrainCmd) _sample;

        if (!CDRSerializer.put_int(cdr, sample.round_id)){
            System.out.println("serialize sample.round_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.subset_size)){
            System.out.println("serialize sample.subset_size failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.epochs)){
            System.out.println("serialize sample.epochs failed.");
            return -2;
        }

        if (!CDRSerializer.put_double(cdr, sample.lr)){
            System.out.println("serialize sample.lr failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.seed)){
            System.out.println("serialize sample.seed failed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        TrainCmd sample = (TrainCmd) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.round_id failed.");
            return -2;
        }
        sample.round_id= tmp_int_obj[0];

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.subset_size failed.");
            return -2;
        }
        sample.subset_size= tmp_int_obj[0];

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.epochs failed.");
            return -2;
        }
        sample.epochs= tmp_int_obj[0];

        if (!CDRDeserializer.get_double_array(cdr, tmp_double_obj, 1)){
            System.out.println("deserialize sample.lr failed.");
            return -2;
        }
        sample.lr= tmp_double_obj[0];

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.seed failed.");
            return -2;
        }
        sample.seed= tmp_int_obj[0];

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        TrainCmd sample = (TrainCmd)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        TrainCmd sample = (TrainCmd)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        TrainCmd sample = (TrainCmd)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("ai_train.TrainCmd");
        if (s_typeCode == null){
            System.out.println("create struct TrainCmd typecode failed.");
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

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UINT);
        if (memberTc == null){
            System.out.println("Get Member subset_size TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "subset_size",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UINT);
        if (memberTc == null){
            System.out.println("Get Member epochs TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
            "epochs",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_DOUBLE);
        if (memberTc == null){
            System.out.println("Get Member lr TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
            "lr",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_UINT);
        if (memberTc == null){
            System.out.println("Get Member seed TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            4,
            4,
            "seed",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        return s_typeCode;
    }

}