package com.example.ocrclient.ai;

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
        int tasksTmpLen = sample.tasks.length();
        System.out.println("sample.tasks.length():" +tasksTmpLen);
        for (int i = 0; i < tasksTmpLen; ++i){
            SingleTaskTypeSupport.get_instance().print_sample(sample.tasks.get_at(i));
        }
        System.out.println("sample.task_num:" + sample.task_num);
        if (sample.worker_id != null){
            System.out.println("sample.worker_id:" + sample.worker_id);
        }
        else{
            System.out.println("sample.worker_id: null");
        }
        KVListTypeSupport.get_instance().print_sample(sample.meta);
        return 0;
    }

    public String get_type_name(){
        return this.type_name;
    }

    public int get_max_sizeI(){
        return 464372;
    }

    public int get_max_key_sizeI(){
        return 464372;
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
        offset += CDRSerializer.get_untype_size(4, offset);
        int tasksLen = sample.tasks.length();
        if (tasksLen != 0){
            for (int i = 0; i < tasksLen; ++i){
                SingleTask curEle = sample.tasks.get_at(i);
                offset += SingleTaskTypeSupport.get_instance().get_sizeI(curEle, cdr, offset);
            }
        }

        offset += CDRSerializer.get_untype_size(4, offset);

        offset += CDRSerializer.get_string_size(sample.worker_id == null ? 0 : sample.worker_id.getBytes().length, offset);

        offset += KVListTypeSupport.get_instance().get_sizeI(sample.meta, cdr, offset);

        return offset - initialAlignment;
    }

    public int serializeI(Object _sample ,long cdr) {
         TaskList sample = (TaskList) _sample;

        if (!CDRSerializer.put_int(cdr, sample.tasks.length())){
            System.out.println("serialize length of sample.tasks failed.");
            return -2;
        }
        for (int i = 0; i < sample.tasks.length(); ++i){
            if (SingleTaskTypeSupport.get_instance().serializeI(sample.tasks.get_at(i),cdr) < 0){
                System.out.println("serialize sample.tasksfailed.");
                return -2;
            }
        }

        if (!CDRSerializer.put_int(cdr, sample.task_num)){
            System.out.println("serialize sample.task_num failed.");
            return -2;
        }

        if (!CDRSerializer.put_string(cdr, sample.worker_id, sample.worker_id == null ? 0 : sample.worker_id.length())){
            System.out.println("serialize sample.worker_id failed.");
            return -2;
        }

        if (KVListTypeSupport.get_instance().serializeI(sample.meta,cdr) < 0){
            System.out.println("serialize sample.metafailed.");
            return -2;
        }

        return 0;
    }

    synchronized public int deserializeI(Object _sample, long cdr){
        TaskList sample = (TaskList) _sample;
        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize length of sample.tasks failed.");
            return -2;
        }
        if (!sample.tasks.ensure_length(tmp_int_obj[0], tmp_int_obj[0])){
            System.out.println("Set maxiumum member sample.tasks failed.");
            return -3;
        }
        SingleTask tmptasks = new SingleTask();
        for (int i = 0; i < sample.tasks.length(); ++i){
            if (SingleTaskTypeSupport.get_instance().deserializeI(tmptasks, cdr) < 0){
                System.out.println("deserialize sample.tasks failed.");
                return -2;
            }
            sample.tasks.set_at(i, tmptasks);
        }

        if (!CDRDeserializer.get_int_array(cdr, tmp_int_obj, 1)){
            System.out.println("deserialize sample.task_num failed.");
            return -2;
        }
        sample.task_num= tmp_int_obj[0];

        sample.worker_id = CDRDeserializer.get_string(cdr);
        if(sample.worker_id ==null){
            System.out.println("deserialize member sample.worker_id failed.");
            return -3;
        }

        if (KVListTypeSupport.get_instance().deserializeI(sample.meta, cdr) < 0){
            System.out.println("deserialize sample.meta failed.");
            return -2;
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

        s_typeCode = factory.create_struct_TC("TaskList");
        if (s_typeCode == null){
            System.out.println("create struct TaskList typecode failed.");
            return s_typeCode;
        }
        int ret = 0;
        TypeCodeImpl memberTc = new TypeCodeImpl();
        TypeCodeImpl eleTc = new TypeCodeImpl();

        memberTc = (TypeCodeImpl)SingleTaskTypeSupport.get_instance().get_typecode();
        if (memberTc != null)
        {
            memberTc = factory.create_sequence_TC(255, memberTc);
        }
        if (memberTc == null){
            System.out.println("Get Member tasks TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            0,
            0,
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

        memberTc = factory.get_primitive_TC(TypeCodeKind.DDS_TK_INT);
        if (memberTc == null){
            System.out.println("Get Member task_num TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            1,
            1,
            "task_num",
            memberTc,
            false,
            false);
        if (ret < 0)
        {
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }

        memberTc = factory.create_string_TC(255);
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

        memberTc = (TypeCodeImpl)KVListTypeSupport.get_instance().get_typecode();
        if (memberTc == null){
            System.out.println("Get Member meta TypeCode failed.");
            factory.delete_TC(s_typeCode);
            s_typeCode = null;
            return null;
        }
        ret = s_typeCode.add_member_to_struct(
            3,
            3,
            "meta",
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