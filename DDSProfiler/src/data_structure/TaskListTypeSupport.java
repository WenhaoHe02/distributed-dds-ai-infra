package data_structure;

import com.zrdds.infrastructure.*;
import com.zrdds.topic.TypeSupport;
import com.zrdds.publication.DataWriter;
import com.zrdds.subscription.DataReader;
import java.io.UnsupportedEncodingException;

public class TaskListTypeSupport extends TypeSupport {
    private String type_name = "TaskList";
    private static TypeCodeImpl s_typeCode = null;
    private static TaskListTypeSupport m_instance = new TaskListTypeSupport();

    private final byte[] tmp_byte_obj = new byte[1];
    private final char[] tmp_char_obj = new char[1];
    private final short[] tmp_short_obj = new short[1];
    private final int[] tmp_int_obj = new int[1];
    private final long[] tmp_long_obj = new long[1];
    private final float[] tmp_float_obj = new float[1];
    private final double[] tmp_double_obj = new double[1];
    private final boolean[] tmp_boolean_obj = new boolean[1];

    
    private TaskListTypeSupport(){}

    
    public static TypeSupport get_instance() { return m_instance; }

    public Object create_sampleI() {
        TaskList sample = new TaskList();
        return sample;
    }

    public void destroy_sampleI(Object sample) {

    }

    public int copy_sampleI(Object dst,Object src) {
        TaskList TaskListDst = (TaskList)dst;
        TaskList TaskListSrc = (TaskList)src;
        TaskListDst.copy(TaskListSrc);
        return 1;
    }

    public int print_sample(Object _sample) {
        if (_sample == null){
            System.out.println("NULL");
            return -1;
        }
        TaskList sample = (TaskList)_sample;
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
        if (sample.assigned_worker_id != null){
            System.out.println("sample.assigned_worker_id:" + sample.assigned_worker_id);
        }
        else{
            System.out.println("sample.assigned_worker_id: null");
        }
        int tasksTmpLen = sample.tasks.length();
        System.out.println("sample.tasks.length():" +tasksTmpLen);
        for (int i = 0; i < tasksTmpLen; ++i){
            data_structure.TaskTypeSupport.get_instance().print_sample(sample.tasks.get_at(i));
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

    public DataReader create_data_reader() {return new TaskListDataReader();}

    public DataWriter create_data_writer() {return new TaskListDataWriter();}

    public TypeCode get_inner_typecode(){
        TypeCode userTypeCode = get_typecode();
        if (userTypeCode == null) return null;
        return userTypeCode.get_impl();
    }

    public int get_sizeI(Object _sample,long cdr, int offset) throws UnsupportedEncodingException {
        int initialAlignment = offset;
        TaskList sample = (TaskList)_sample;
        offset += CDRSerializer.get_string_size(sample.batch_id == null ? 0 : sample.batch_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.model_id == null ? 0 : sample.model_id.getBytes().length, offset);

        offset += CDRSerializer.get_string_size(sample.assigned_worker_id == null ? 0 : sample.assigned_worker_id.getBytes().length, offset);

        offset += CDRSerializer.get_untype_size(4, offset);
        int tasksLen = sample.tasks.length();
        if (tasksLen != 0){
            for (int i = 0; i < tasksLen; ++i){
                data_structure.Task curEle = sample.tasks.get_at(i);
                offset += data_structure.TaskTypeSupport.get_instance().get_sizeI(curEle, cdr, offset);
            }
        }

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         TaskList sample = (TaskList) _sample;

        if (!CDRSerializer.put_string(cdr, sample.batch_id, sample.batch_id == null ? 0 : sample.batch_id.length())){
            System.out.println("serialize sample.batch_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.model_id, sample.model_id == null ? 0 : sample.model_id.length())){
            System.out.println("serialize sample.model_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.assigned_worker_id, sample.assigned_worker_id == null ? 0 : sample.assigned_worker_id.length())){
            System.out.println("serialize sample.assigned_worker_id failed.");
            return -2;
        }

        if (!CDRSerializer.put_int(cdr, sample.tasks.length())){
            System.out.println("serialize length of sample.tasks failed.");
            return -2;
        }
        for (int i = 0; i < sample.tasks.length(); ++i){
            if (data_structure.TaskTypeSupport.get_instance().serializeI(sample.tasks.get_at(i),cdr) < 0){
                System.out.println("serialize sample.tasksfailed.");
                return -2;
            }
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        TaskList sample = (TaskList) _sample;
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

        sample.assigned_worker_id = CDRDeserializer.get_string(cdr);
        if(sample.assigned_worker_id ==null){
            System.out.println("deserialize member sample.assigned_worker_id failed.");
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
        data_structure.Task tmptasks = new data_structure.Task();
        for (int i = 0; i < sample.tasks.length(); ++i){
            if (data_structure.TaskTypeSupport.get_instance().deserializeI(tmptasks, cdr) < 0){
                System.out.println("deserialize sample.tasks failed.");
                return -2;
            }
            sample.tasks.set_at(i, tmptasks);
        }

        return 0;
    }

    public int get_key_sizeI(Object _sample,long cdr,int offset)throws UnsupportedEncodingException {
        int initialAlignment = offset;
        TaskList sample = (TaskList)_sample;
        offset += get_sizeI(sample, cdr, offset);
        return offset - initialAlignment;
    }

    public int serialize_keyI(Object _sample, long cdr){
        TaskList sample = (TaskList)_sample;
        return 0;
    }

    public int deserialize_keyI(Object _sample, long cdr) {
        TaskList sample = (TaskList)_sample;
        return 0;
    }

    public TypeCode get_typecode(){
        if (s_typeCode != null) {
            return s_typeCode;
        }
        TypeCodeFactory factory = TypeCodeFactory.get_instance();

        s_typeCode = factory.create_struct_TC("data_structure.TaskList");
        if (s_typeCode == null){
            System.out.println("create struct TaskList typecode failed.");
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
            System.out.println("Get Member assigned_worker_id TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            2,
            2,
            "assigned_worker_id",
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

        memberTc = (TypeCodeImpl)data_structure.TaskTypeSupport.get_instance().get_typecode();
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(0xffffffff, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member tasks TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
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