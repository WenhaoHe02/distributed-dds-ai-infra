// py_ai_infer.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
#include <cstring>
#include <stdexcept>
#include <string>
#include <memory>

#include "ai_infer.h"  // 生成头，类型都在 namespace data_structure 里
#include "ai_inferDataReader.h"
#include "ai_inferDataWriter.h"


#include "ai_infer.h"

namespace py = pybind11;
namespace ds = data_structure;
// -------------------- Bytes <-> Python bytes --------------------
static void py_to_bytes(const py::bytes& pybytes, ds::Bytes& dds_bytes) {
    std::string temp = pybytes;
    DDS_ULong len = static_cast<DDS_ULong>(temp.size());
    if (len > 0) {
        dds_bytes.value.ensure_length(len, len);
        dds_bytes.value.from_array(reinterpret_cast<const DDS_Octet*>(temp.data()), len);
    } else {
        dds_bytes.value.ensure_length(0, 0);
    }
}

static py::bytes bytes_to_py(const ds::Bytes& dds_bytes) {
    DDS_ULong len = dds_bytes.value.length();
    if (len == 0) return py::bytes();


    const DDS_Octet* buf = dds_bytes.value.get_contiguous_buffer();
    if (!buf) return py::bytes();
    return py::bytes(reinterpret_cast<const char*>(buf), static_cast<size_t>(len));
}

// -------------------- C-string field helpers (max 255) --------------------
static std::string get_cstr(const DDS_Char* p) {
    return p ? std::string(p) : std::string();
}

static constexpr size_t kMaxStr = 255;
static constexpr size_t kBufLen = kMaxStr + 1;

static void set_cstr(DDS_Char*& p, const std::string& s) {
    if (s.size() > kMaxStr) throw std::runtime_error("string too long (>255)");
    if (!p) {                          // 懒分配
        p = new DDS_Char[kBufLen]();
    }
    std::memcpy(p, s.data(), s.size());
    p[s.size()] = '\0';
}

static void copy_bytes(const ds::Bytes& src, ds::Bytes& dst) {
    DDS_ULong len = src.value.length();
    dst.value.ensure_length(len, len);
    const DDS_Octet* buf = src.value.get_contiguous_buffer();
    if (len) dst.value.from_array(buf, len);
}

template <typename Seq, typename Elem>
static void copy_seq(const Seq& src, Seq& dst) {
    DDS_ULong len = src.length();
    dst.ensure_length(len, len);
    for (DDS_ULong i = 0; i < len; ++i) {
        const Elem& e = src.get_at(i);
        dst.set_at(i, e);   // 生成器的 set_at 通常会做元素级深拷
    }
}

// -------------------- Seq 映射（Base -> Wrapper） --------------------
template <typename Seq, typename Base, typename Wrapper>
void bind_sequence_mapped(py::module_& m, const char* name) {
    py::class_<Seq>(m, name)
        .def(py::init<DDS_ULong>(), py::arg("max") = 16)
        .def("length", [](const Seq& self) { return static_cast<size_t>(self.length()); })
        .def("get_at", [](Seq& self, size_t i) {
        if (i >= self.length()) throw py::index_error();
        const Base& elem = self.get_at(static_cast<DDS_ULong>(i));
        auto wrapper = new Wrapper();
        deep_copy(elem, *wrapper);
        return wrapper;
             }, py::return_value_policy::take_ownership)
        .def("set_at", [](Seq& self, size_t i, const Wrapper& val) {
                 if (i >= self.length()) throw py::index_error();
                 const Base& base_val = val;
                 if (!self.set_at(static_cast<DDS_ULong>(i), base_val))
                     throw std::runtime_error("set_at failed");
             })
                 .def("append", [](Seq& self, const Wrapper& val) {
                 if (!self.append(static_cast<const Base&>(val)))
                     throw std::runtime_error("append failed");
                      })
                 .def("clear", [](Seq& self) {
                          if (!self.clear()) throw std::runtime_error("clear failed");
                      })
                          .def("ensure_length", [](Seq& self, size_t length, size_t max) {
                          if (!self.ensure_length(static_cast<DDS_ULong>(length), static_cast<DDS_ULong>(max)))
                              throw std::runtime_error("ensure_length failed");
                               })
                          .def("to_array", [](Seq& self) {
                                   py::list result;
                                   DDS_ULong len = self.length();
                                   for (DDS_ULong i = 0; i < len; ++i) {
                                       const Base& elem = self.get_at(i);
                                       auto wrapper = new Wrapper();
                                       *static_cast<Base*>(wrapper) = elem;
                                       result.append(py::cast(wrapper, py::return_value_policy::take_ownership));
                                   }
                                   return result;
                               })
                                   .def("from_array", [](Seq& self, const py::list& list) {
                                   DDS_ULong len = static_cast<DDS_ULong>(list.size());
                                   self.ensure_length(len, len);
                                   for (DDS_ULong i = 0; i < len; ++i) {
                                       const Wrapper& wrapper = list[i].cast<const Wrapper&>();
                                       self.set_at(i, static_cast<const Base&>(wrapper));
                                   }
                                   return true;
                                        });
}

struct TaskWrapper : ds::Task {
    TaskWrapper() {
        ds::TaskInitialize(this);
    }
    ~TaskWrapper() {
        delete[] this->request_id; this->request_id = nullptr;
        delete[] this->task_id;    this->task_id = nullptr;
        delete[] this->client_id;  this->client_id = nullptr;
        ds::TaskFinalize(this);
    }
};

struct ClaimWrapper : ds::Claim {
    ClaimWrapper() {
        ds::ClaimInitialize(this);
    }
    ~ClaimWrapper() {
        delete[] this->batch_id;  this->batch_id = nullptr;
        delete[] this->worker_id; this->worker_id = nullptr;
        ds::ClaimFinalize(this);
    }
};

struct OpenBatchWrapper : ds::OpenBatch {
    OpenBatchWrapper() {
        ds::OpenBatchInitialize(this);
    }
    ~OpenBatchWrapper() {
        delete[] this->batch_id; this->batch_id = nullptr;
        delete[] this->model_id; this->model_id = nullptr;
        ds::OpenBatchFinalize(this);
    }
};

struct GrantWrapper : ds::Grant {
    GrantWrapper() {
        ds::GrantInitialize(this);
    }
    ~GrantWrapper() {
        delete[] this->batch_id;          this->batch_id = nullptr;
        delete[] this->winner_worker_id;  this->winner_worker_id = nullptr;
        ds::GrantFinalize(this);
    }
};

struct WorkerTaskResultWrapper : ds::WorkerTaskResult {
    WorkerTaskResultWrapper() {
        ds::WorkerTaskResultInitialize(this);
    }
    ~WorkerTaskResultWrapper() {
        delete[] this->request_id; this->request_id = nullptr;
        delete[] this->task_id;    this->task_id = nullptr;
        delete[] this->client_id;  this->client_id = nullptr;
        delete[] this->status;     this->status = nullptr;
        ds::WorkerTaskResultFinalize(this);
    }
};

struct TaskListWrapper : ds::TaskList {
    TaskListWrapper() {
        ds::TaskListInitialize(this);
    }
    ~TaskListWrapper() {
        delete[] this->batch_id;           this->batch_id = nullptr;
        delete[] this->model_id;           this->model_id = nullptr;
        delete[] this->assigned_worker_id; this->assigned_worker_id = nullptr;
        ds::TaskListFinalize(this);
    }
};

struct WorkerResultWrapper : ds::WorkerResult {
    WorkerResultWrapper() {
        ds::WorkerResultInitialize(this);
    }
    ~WorkerResultWrapper() {
        delete[] this->batch_id; this->batch_id = nullptr;
        delete[] this->model_id; this->model_id = nullptr;
        delete[] this->worker_id; this->worker_id = nullptr;
        ds::WorkerResultFinalize(this);
    }
};


static void deep_copy(const ds::Task& src, TaskWrapper& dst) {
    if (src.request_id) set_cstr(dst.request_id, src.request_id);
    if (src.task_id)    set_cstr(dst.task_id, src.task_id);
    if (src.client_id)  set_cstr(dst.client_id, src.client_id);
    copy_bytes(src.payload, dst.payload);
}

static void deep_copy(const ds::Claim& src, ClaimWrapper& dst) {
    if (src.batch_id)  set_cstr(dst.batch_id, src.batch_id);
    if (src.worker_id) set_cstr(dst.worker_id, src.worker_id);
    dst.queue_length = src.queue_length;
}

static void deep_copy(const ds::OpenBatch& src, OpenBatchWrapper& dst) {
    if (src.batch_id) set_cstr(dst.batch_id, src.batch_id);
    if (src.model_id) set_cstr(dst.model_id, src.model_id);
    dst.size = src.size;
    dst.create_ts_ms = src.create_ts_ms;
}

static void deep_copy(const ds::Grant& src, GrantWrapper& dst) {
    if (src.batch_id)         set_cstr(dst.batch_id, src.batch_id);
    if (src.winner_worker_id) set_cstr(dst.winner_worker_id, src.winner_worker_id);
}

static void deep_copy(const ds::WorkerTaskResult& src, WorkerTaskResultWrapper& dst) {
    if (src.request_id) set_cstr(dst.request_id, src.request_id);
    if (src.task_id)    set_cstr(dst.task_id, src.task_id);
    if (src.client_id)  set_cstr(dst.client_id, src.client_id);
    if (src.status)     set_cstr(dst.status, src.status);
    copy_bytes(src.output_blob, dst.output_blob);
}

static void deep_copy(const ds::TaskList& src, TaskListWrapper& dst) {
    if (src.batch_id)           set_cstr(dst.batch_id, src.batch_id);
    if (src.model_id)           set_cstr(dst.model_id, src.model_id);
    if (src.assigned_worker_id) set_cstr(dst.assigned_worker_id, src.assigned_worker_id);
    // 拷贝内部序列（元素类型是 ds::Task）
    copy_seq<ds::TaskSeq, ds::Task>(src.tasks, dst.tasks);
}
static void deep_copy(const ds::WorkerResult& src, WorkerResultWrapper& dst) {
    if (src.batch_id) set_cstr(dst.batch_id, src.batch_id);
    if (src.model_id) set_cstr(dst.model_id, src.model_id);
    if (src.worker_id) set_cstr(dst.worker_id, src.worker_id);
    // 拷贝内部序列（元素类型是 ds::WorkerTaskResult）
    copy_seq<ds::WorkerTaskResultSeq, ds::WorkerTaskResult>(src.results, dst.results);
}

// ---- Writer deleters ----
struct TaskDataWriterDeleter {
    void operator()(ds::TaskDataWriter*)             const {}
};
struct ClaimDataWriterDeleter {
    void operator()(ds::ClaimDataWriter*)            const {}
};
struct OpenBatchDataWriterDeleter {
    void operator()(ds::OpenBatchDataWriter*)        const {}
};
struct GrantDataWriterDeleter {
    void operator()(ds::GrantDataWriter*)            const {}
};
struct TaskListDataWriterDeleter {
    void operator()(ds::TaskListDataWriter*)         const {}
};
struct WorkerTaskResultDataWriterDeleter {
    void operator()(ds::WorkerTaskResultDataWriter*) const {}
};
struct WorkerResultDataWriterDeleter {
    void operator()(ds::WorkerResultDataWriter*)     const {}
};

// ---- Reader deleters ----
struct TaskDataReaderDeleter {
    void operator()(ds::TaskDataReader*)             const {}
};
struct ClaimDataReaderDeleter {
    void operator()(ds::ClaimDataReader*)            const {}
};
struct OpenBatchDataReaderDeleter {
    void operator()(ds::OpenBatchDataReader*)        const {}
};
struct GrantDataReaderDeleter {
    void operator()(ds::GrantDataReader*)            const {}
};
struct TaskListDataReaderDeleter {
    void operator()(ds::TaskListDataReader*)         const {}
};
struct WorkerTaskResultDataReaderDeleter {
    void operator()(ds::WorkerTaskResultDataReader*) const {}
};
struct WorkerResultDataReaderDeleter {
    void operator()(ds::WorkerResultDataReader*)     const {}
};

// ========== DataWriter 绑定：write(wrapper) ==========
template <typename DDSWriterT, typename MsgT, typename WrapperT, typename DeleterT>
void bind_datawriter(py::module_& m, const char* py_name) {
    py::class_<DDSWriterT, DDS::DataWriter, std::unique_ptr<DDSWriterT, DeleterT>>(m, py_name)
        .def("write", [](DDSWriterT& writer, const WrapperT& w) {
        const MsgT& msg = static_cast<const MsgT&>(w);
        return writer.write(msg, DDS::HANDLE_NIL_NATIVE);
             });
}

// ========== DataReader 绑定：read_list / take_list ==========
template <typename DDSReaderT, typename MsgT, typename SeqT, typename WrapperT, typename DeleterT>
void bind_datareader(py::module_& m, const char* py_name) {
    py::class_<DDSReaderT, DDS::DataReader, std::unique_ptr<DDSReaderT, DeleterT>>(m, py_name)
        .def("read_list", [](DDSReaderT& r, int max_samples = -1) {
        SeqT dataSeq; DDS_SampleInfoSeq infoSeq;
        auto rc = r.read(dataSeq, infoSeq,
                         (DDS::Long)max_samples,
                         DDS::ANY_SAMPLE_STATE, DDS::ANY_VIEW_STATE, DDS::ANY_INSTANCE_STATE);
        py::list out;
        DDS_ULong n = dataSeq.length();
        for (DDS_ULong i = 0; i < n; ++i) {
            const MsgT& elem = dataSeq.get_at(i);
            auto w = new WrapperT();
            deep_copy(elem, *w);
            out.append(py::cast(w, py::return_value_policy::take_ownership));
        }
        r.return_loan(dataSeq, infoSeq);
        return out;  // 仅返回数据列表，rc 如需也可一起返回
             }, py::arg("max_samples") = -1)
        .def("take_list", [](DDSReaderT& r, int max_samples = -1) {
                 SeqT dataSeq; DDS_SampleInfoSeq infoSeq;
                 auto rc = r.take(dataSeq, infoSeq,
                                  (DDS::Long)max_samples,
                                  DDS::ANY_SAMPLE_STATE, DDS::ANY_VIEW_STATE, DDS::ANY_INSTANCE_STATE);
                 py::list out;
                 DDS_ULong n = dataSeq.length();
                 for (DDS_ULong i = 0; i < n; ++i) {
                     const MsgT& elem = dataSeq.get_at(i);
                     auto w = new WrapperT();
                     deep_copy(elem, *w);
                     out.append(py::cast(w, py::return_value_policy::take_ownership));
                 }
                 r.return_loan(dataSeq, infoSeq);
                 return out;
             }, py::arg("max_samples") = -1);
}

PYBIND11_MODULE(dds_infer_pybindings, m) {
    // --------- Task（TaskList 内会用到）---------
    py::class_<TaskWrapper>(m, "Task")
        .def(py::init<>())
        .def_property("request_id",
                      [](const TaskWrapper& self) { return get_cstr(self.request_id); },
                      [](TaskWrapper& self, const std::string& s) { set_cstr(self.request_id, s); })
        .def_property("task_id",
                      [](const TaskWrapper& self) { return get_cstr(self.task_id); },
                      [](TaskWrapper& self, const std::string& s) { set_cstr(self.task_id, s); })
        .def_property("client_id",
                      [](const TaskWrapper& self) { return get_cstr(self.client_id); },
                      [](TaskWrapper& self, const std::string& s) { set_cstr(self.client_id, s); })
        .def_property("payload",
                      [](const TaskWrapper& self) { return bytes_to_py(self.payload); },
                      [](TaskWrapper& self, const py::bytes& b) { py_to_bytes(b, self.payload); });

    // 对应 C++ 的 ds::TaskSeq
    bind_sequence_mapped<ds::TaskSeq, ds::Task, TaskWrapper>(m, "TaskSeq");

    // --------- Claim ---------
    py::class_<ClaimWrapper>(m, "Claim")
        .def(py::init<>())
        .def_property("batch_id",
                      [](const ClaimWrapper& self) { return get_cstr(self.batch_id); },
                      [](ClaimWrapper& self, const std::string& s) { set_cstr(self.batch_id, s); })
        .def_property("worker_id",
                      [](const ClaimWrapper& self) { return get_cstr(self.worker_id); },
                      [](ClaimWrapper& self, const std::string& s) { set_cstr(self.worker_id, s); })
        .def_readwrite("queue_length", &ClaimWrapper::queue_length);
    bind_sequence_mapped<ds::ClaimSeq, ds::Claim, ClaimWrapper>(m, "ClaimSeq");

    // --------- OpenBatch ---------
    py::class_<OpenBatchWrapper>(m, "OpenBatch")
        .def(py::init<>())
        .def_property("batch_id",
                      [](const OpenBatchWrapper& self) { return get_cstr(self.batch_id); },
                      [](OpenBatchWrapper& self, const std::string& s) { set_cstr(self.batch_id, s); })
        .def_property("model_id",
                      [](const OpenBatchWrapper& self) { return get_cstr(self.model_id); },
                      [](OpenBatchWrapper& self, const std::string& s) { set_cstr(self.model_id, s); })
        .def_readwrite("size", &OpenBatchWrapper::size)
        .def_readwrite("create_ts_ms", &OpenBatchWrapper::create_ts_ms);
    bind_sequence_mapped<ds::OpenBatchSeq, ds::OpenBatch, OpenBatchWrapper>(m, "OpenBatchSeq");

    // --------- Grant ---------
    py::class_<GrantWrapper>(m, "Grant")
        .def(py::init<>())
        .def_property("batch_id",
                      [](const GrantWrapper& self) { return get_cstr(self.batch_id); },
                      [](GrantWrapper& self, const std::string& s) { set_cstr(self.batch_id, s); })
        .def_property("winner_worker_id",
                      [](const GrantWrapper& self) { return get_cstr(self.winner_worker_id); },
                      [](GrantWrapper& self, const std::string& s) { set_cstr(self.winner_worker_id, s); });
    bind_sequence_mapped<ds::GrantSeq, ds::Grant, GrantWrapper>(m, "GrantSeq");

    // --------- WorkerTaskResult（WorkerResult.results 用）---------
    py::class_<WorkerTaskResultWrapper>(m, "WorkerTaskResult")
        .def(py::init<>())
        .def_property("request_id",
                      [](const WorkerTaskResultWrapper& self) { return get_cstr(self.request_id); },
                      [](WorkerTaskResultWrapper& self, const std::string& s) { set_cstr(self.request_id, s); })
        .def_property("task_id",
                      [](const WorkerTaskResultWrapper& self) { return get_cstr(self.task_id); },
                      [](WorkerTaskResultWrapper& self, const std::string& s) { set_cstr(self.task_id, s); })
        .def_property("client_id",
                      [](const WorkerTaskResultWrapper& self) { return get_cstr(self.client_id); },
                      [](WorkerTaskResultWrapper& self, const std::string& s) { set_cstr(self.client_id, s); })
        .def_property("status",
                      [](const WorkerTaskResultWrapper& self) { return get_cstr(self.status); },
                      [](WorkerTaskResultWrapper& self, const std::string& s) { set_cstr(self.status, s); })
        .def_property("output_blob",
                      [](const WorkerTaskResultWrapper& self) { return bytes_to_py(self.output_blob); },
                      [](WorkerTaskResultWrapper& self, const py::bytes& b) { py_to_bytes(b, self.output_blob); });
    bind_sequence_mapped<ds::WorkerTaskResultSeq, ds::WorkerTaskResult, WorkerTaskResultWrapper>(m, "WorkerTaskResultSeq");

    // --------- TaskList ---------
    py::class_<TaskListWrapper>(m, "TaskList")
        .def(py::init<>())
        .def_property("batch_id",
                      [](const TaskListWrapper& self) { return get_cstr(self.batch_id); },
                      [](TaskListWrapper& self, const std::string& s) { set_cstr(self.batch_id, s); })
        .def_property("model_id",
                      [](const TaskListWrapper& self) { return get_cstr(self.model_id); },
                      [](TaskListWrapper& self, const std::string& s) { set_cstr(self.model_id, s); })
        .def_property("assigned_worker_id",
                      [](const TaskListWrapper& self) { return get_cstr(self.assigned_worker_id); },
                      [](TaskListWrapper& self, const std::string& s) { set_cstr(self.assigned_worker_id, s); })
        // 友好版：把内部 TaskSeq 映射为 Python list[Task]
        .def_property("tasks",
                      [](TaskListWrapper& self) {
                          py::list result;
                          DDS_ULong len = self.tasks.length();
                          for (DDS_ULong i = 0; i < len; ++i) {
                              const ds::Task& elem = self.tasks.get_at(i);
                              auto wrapper = new TaskWrapper();
                              deep_copy(elem, *wrapper);
                              result.append(py::cast(wrapper, py::return_value_policy::take_ownership));
                          }
                          return result;
                      },
                      [](TaskListWrapper& self, const py::list& list) {
                          DDS_ULong len = static_cast<DDS_ULong>(list.size());
                          self.tasks.ensure_length(len, len);
                          for (DDS_ULong i = 0; i < len; ++i) {
                              const TaskWrapper& w = list[i].cast<const TaskWrapper&>();
                              self.tasks.set_at(i, static_cast<const ds::Task&>(w));
                          }
                      }
                      );
    bind_sequence_mapped<ds::TaskListSeq, ds::TaskList, TaskListWrapper>(m, "TaskListSeq");

    // --------- Worker（WorkerResult）---------
    py::class_<WorkerResultWrapper>(m, "WorkerResult")
        .def(py::init<>())
        .def_property("batch_id",
                      [](const WorkerResultWrapper& self) { return get_cstr(self.batch_id); },
                      [](WorkerResultWrapper& self, const std::string& s) { set_cstr(self.batch_id, s); })
        .def_property("model_id",
                      [](const WorkerResultWrapper& self) { return get_cstr(self.model_id); },
                      [](WorkerResultWrapper& self, const std::string& s) { set_cstr(self.model_id, s); })
        .def_property("worker_id",
                      [](const WorkerResultWrapper& self) { return get_cstr(self.worker_id); },
                      [](WorkerResultWrapper& self, const std::string& s) { set_cstr(self.worker_id, s); })
        // 友好版：内部 WorkerTaskResultSeq 映射为 Python list[WorkerTaskResult]
        .def_property("results",
                      [](WorkerResultWrapper& self) {
                          py::list result;
                          DDS_ULong len = self.results.length();
                          for (DDS_ULong i = 0; i < len; ++i) {
                              const ds::WorkerTaskResult& elem = self.results.get_at(i);
                              auto wrapper = new WorkerTaskResultWrapper();
                              deep_copy(elem, *wrapper);
                              result.append(py::cast(wrapper, py::return_value_policy::take_ownership));
                          }
                          return result;
                      },
                      [](WorkerResultWrapper& self, const py::list& list) {
                          DDS_ULong len = static_cast<DDS_ULong>(list.size());
                          self.results.ensure_length(len, len);
                          for (DDS_ULong i = 0; i < len; ++i) {
                              const WorkerTaskResultWrapper& w = list[i].cast<const WorkerTaskResultWrapper&>();
                              self.results.set_at(i, static_cast<const ds::WorkerTaskResult&>(w));
                          }
                      }
                      );
    bind_sequence_mapped<ds::WorkerResultSeq, ds::WorkerResult, WorkerResultWrapper>(m, "WorkerResultSeq");


    // --------- Writers ---------
    bind_datawriter<ds::TaskDataWriter, ds::Task, TaskWrapper, TaskDataWriterDeleter>(m, "TaskDataWriter");
    bind_datawriter<ds::ClaimDataWriter, ds::Claim, ClaimWrapper, ClaimDataWriterDeleter>(m, "ClaimDataWriter");
    bind_datawriter<ds::OpenBatchDataWriter, ds::OpenBatch, OpenBatchWrapper, OpenBatchDataWriterDeleter>(m, "OpenBatchDataWriter");
    bind_datawriter<ds::GrantDataWriter, ds::Grant, GrantWrapper, GrantDataWriterDeleter>(m, "GrantDataWriter");
    bind_datawriter<ds::TaskListDataWriter, ds::TaskList, TaskListWrapper, TaskListDataWriterDeleter>(m, "TaskListDataWriter");
    bind_datawriter<ds::WorkerTaskResultDataWriter, ds::WorkerTaskResult, WorkerTaskResultWrapper, WorkerTaskResultDataWriterDeleter>(m, "WorkerTaskResultDataWriter");
    bind_datawriter<ds::WorkerResultDataWriter, ds::WorkerResult, WorkerResultWrapper, WorkerResultDataWriterDeleter>(m, "WorkerResultDataWriter");

    // --------- Readers ---------
    bind_datareader<ds::TaskDataReader, ds::Task, ds::TaskSeq, TaskWrapper, TaskDataReaderDeleter>(m, "TaskDataReader");
    bind_datareader<ds::ClaimDataReader, ds::Claim, ds::ClaimSeq, ClaimWrapper, ClaimDataReaderDeleter>(m, "ClaimDataReader");
    bind_datareader<ds::OpenBatchDataReader, ds::OpenBatch, ds::OpenBatchSeq, OpenBatchWrapper, OpenBatchDataReaderDeleter>(m, "OpenBatchDataReader");
    bind_datareader<ds::GrantDataReader, ds::Grant, ds::GrantSeq, GrantWrapper, GrantDataReaderDeleter>(m, "GrantDataReader");
    bind_datareader<ds::TaskListDataReader, ds::TaskList, ds::TaskListSeq, TaskListWrapper, TaskListDataReaderDeleter>(m, "TaskListDataReader");
    bind_datareader<ds::WorkerTaskResultDataReader, ds::WorkerTaskResult, ds::WorkerTaskResultSeq, WorkerTaskResultWrapper, WorkerTaskResultDataReaderDeleter>(m, "WorkerTaskResultDataReader");
    bind_datareader<ds::WorkerResultDataReader, ds::WorkerResult, ds::WorkerResultSeq, WorkerResultWrapper, WorkerResultDataReaderDeleter>(m, "WorkerResultDataReader");
}
