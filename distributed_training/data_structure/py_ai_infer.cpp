// py_ai_infer.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
#include <cstring>
#include <stdexcept>
#include <string>
#include <memory>

#include "ai_infer.h"  // 生成头，类型都在 namespace data_structure 里

namespace py = pybind11;

// ---- 小工具 ----
static inline void assert_inited(const DDS_Char* p, const char* field) {
    if (!p) throw std::runtime_error(std::string("uninitialized field: ") + field);
}
static inline void set_bounded_string(DDS_Char*& dst, const std::string& v, const char* field) {
    constexpr size_t kMax = 255;
    if (v.size() > kMax) throw std::runtime_error(std::string(field) + " exceeds max length 255");
    assert_inited(dst, field);
#if defined(_MSC_VER)
    strncpy_s(dst, kMax + 1, v.c_str(), v.size());
#else
    std::strncpy(dst, v.c_str(), kMax);
    dst[kMax] = '\0';
#endif
    if (v.empty()) dst[0] = '\0';
}
static inline std::string get_string(const DDS_Char* p) {
    return p ? std::string(p) : std::string();
}

// Bytes <-> py::bytes
static py::bytes bytes_to_py(const data_structure::Bytes& seq) {
    DDS_ULong n = data_structure::Bytes_get_length(const_cast<data_structure::Bytes*>(&seq));
    std::string out; out.resize(n);
    for (DDS_ULong i = 0; i < n; ++i) {
        out[i] = static_cast<char>(*data_structure::Bytes_get_reference(const_cast<data_structure::Bytes*>(&seq), i));
    }
    return py::bytes(out);
}
static void py_to_bytes(data_structure::Bytes& seq, py::handle obj) {
    py::bytes b = py::bytes(obj);
    std::string buf = b;
    DDS_ULong n = static_cast<DDS_ULong>(buf.size());
    if (!data_structure::Bytes_ensure_length(&seq, n, n))
        throw std::runtime_error("Bytes_ensure_length failed");
    for (DDS_ULong i = 0; i < n; ++i) {
        *data_structure::Bytes_get_reference(&seq, i) =
            static_cast<DDS_Octet>(static_cast<unsigned char>(buf[i]));
    }
}

// ---- RAII holder（调用 Finalize 再 delete）----
template <typename T, void (*FinalizeFn)(T*)>
struct FinalizeDeleter {
    void operator()(T* p) const noexcept {
        if (p) {
            FinalizeFn(p); delete p;
        }
    }
};

using Task = data_structure::Task;
using OpenBatch = data_structure::OpenBatch;
using Claim = data_structure::Claim;
using Grant = data_structure::Grant;
using TaskList = data_structure::TaskList;
using WorkerTaskResult = data_structure::WorkerTaskResult;
using WorkerResult = data_structure::WorkerResult;

using TaskHolder = std::unique_ptr<Task, FinalizeDeleter<Task, data_structure::TaskFinalize>>;
using OpenBatchHolder = std::unique_ptr<OpenBatch, FinalizeDeleter<OpenBatch, data_structure::OpenBatchFinalize>>;
using ClaimHolder = std::unique_ptr<Claim, FinalizeDeleter<Claim, data_structure::ClaimFinalize>>;
using GrantHolder = std::unique_ptr<Grant, FinalizeDeleter<Grant, data_structure::GrantFinalize>>;
using TaskListHolder = std::unique_ptr<TaskList, FinalizeDeleter<TaskList, data_structure::TaskListFinalize>>;
using WorkerTaskResultHolder = std::unique_ptr<WorkerTaskResult, FinalizeDeleter<WorkerTaskResult, data_structure::WorkerTaskResultFinalize>>;
using WorkerResultHolder = std::unique_ptr<WorkerResult, FinalizeDeleter<WorkerResult, data_structure::WorkerResultFinalize>>;

// ---- 序列互转 ----
static py::list taskseq_to_list(const data_structure::TaskSeq& seq) {
    py::list L;
    DDS_ULong n = data_structure::TaskSeq_get_length(const_cast<data_structure::TaskSeq*>(&seq));
    for (DDS_ULong i = 0; i < n; ++i) {
        auto* elem = new Task();
        if (!data_structure::TaskInitialize(elem)) {
            delete elem; throw std::runtime_error("TaskInitialize failed");
        }
        if (!data_structure::TaskCopy(elem, &*data_structure::TaskSeq_get_reference(const_cast<data_structure::TaskSeq*>(&seq), i))) {
            delete elem; throw std::runtime_error("TaskCopy failed");
        }
        L.append(py::cast(TaskHolder(elem)));
    }
    return L;
}
static void list_to_taskseq(data_structure::TaskSeq& seq, py::handle h) {
    py::sequence s = py::reinterpret_borrow<py::sequence>(h);
    DDS_ULong n = static_cast<DDS_ULong>(py::len(s));
    if (!data_structure::TaskSeq_ensure_length(&seq, n, n))
        throw std::runtime_error("TaskSeq_ensure_length failed");
    for (DDS_ULong i = 0; i < n; ++i) {
        Task& src = py::cast<Task&>(s[i]);
        if (!data_structure::TaskCopy(&*data_structure::TaskSeq_get_reference(&seq, i), &src))
            throw std::runtime_error("TaskCopy into seq failed");
    }
}

static py::list wtrseq_to_list(const data_structure::WorkerTaskResultSeq& seq) {
    py::list L;
    DDS_ULong n = data_structure::WorkerTaskResultSeq_get_length(const_cast<data_structure::WorkerTaskResultSeq*>(&seq));
    for (DDS_ULong i = 0; i < n; ++i) {
        auto* elem = new WorkerTaskResult();
        if (!data_structure::WorkerTaskResultInitialize(elem)) {
            delete elem; throw std::runtime_error("WorkerTaskResultInitialize failed");
        }
        if (!data_structure::WorkerTaskResultCopy(elem, &*data_structure::WorkerTaskResultSeq_get_reference(const_cast<data_structure::WorkerTaskResultSeq*>(&seq), i))) {
            delete elem; throw std::runtime_error("WorkerTaskResultCopy failed");
        }
        L.append(py::cast(WorkerTaskResultHolder(elem)));
    }
    return L;
}
static void list_to_wtrseq(data_structure::WorkerTaskResultSeq& seq, py::handle h) {
    py::sequence s = py::reinterpret_borrow<py::sequence>(h);
    DDS_ULong n = static_cast<DDS_ULong>(py::len(s));
    if (!data_structure::WorkerTaskResultSeq_ensure_length(&seq, n, n))
        throw std::runtime_error("WorkerTaskResultSeq_ensure_length failed");
    for (DDS_ULong i = 0; i < n; ++i) {
        WorkerTaskResult& src = py::cast<WorkerTaskResult&>(s[i]);
        if (!data_structure::WorkerTaskResultCopy(&*data_structure::WorkerTaskResultSeq_get_reference(&seq, i), &src))
            throw std::runtime_error("WorkerTaskResultCopy into seq failed");
    }
}

// ---- 模块 ----
PYBIND11_MODULE(py_ai_infer, m) {
    m.doc() = "pybind11 bindings for ai_infer data types (no DDS I/O)";

    // Task
    py::class_<Task, TaskHolder>(m, "Task")
        .def(py::init([] {
        auto* p = new Task();
        if (!data_structure::TaskInitialize(p)) {
            delete p; throw std::runtime_error("TaskInitialize failed");
        }
        return p;
                      }))
        .def_property("request_id", [](Task& s) { return get_string(s.request_id); },
                      [](Task& s, const std::string& v) { set_bounded_string(s.request_id, v, "request_id"); })
                          .def_property("task_id", [](Task& s) { return get_string(s.task_id); },
                                        [](Task& s, const std::string& v) { set_bounded_string(s.task_id, v, "task_id"); })
                          .def_property("client_id", [](Task& s) { return get_string(s.client_id); },
                                        [](Task& s, const std::string& v) { set_bounded_string(s.client_id, v, "client_id"); })
                          .def_property("payload", [](Task& s) { return bytes_to_py(s.payload); },
                                        [](Task& s, py::handle obj) { py_to_bytes(s.payload, obj); })
                          .def("to_dict", [](Task& s) {
                          py::dict d;
                          d["request_id"] = get_string(s.request_id);
                          d["task_id"] = get_string(s.task_id);
                          d["client_id"] = get_string(s.client_id);
                          d["payload"] = bytes_to_py(s.payload);
                          return d;
                               });

                      // OpenBatch
                      py::class_<OpenBatch, OpenBatchHolder>(m, "OpenBatch")
                          .def(py::init([] {
                          auto* p = new OpenBatch();
                          if (!data_structure::OpenBatchInitialize(p)) {
                              delete p; throw std::runtime_error("OpenBatchInitialize failed");
                          }
                          return p;
                                        }))
                          .def_property("batch_id", [](OpenBatch& s) { return get_string(s.batch_id); },
                                        [](OpenBatch& s, const std::string& v) { set_bounded_string(s.batch_id, v, "batch_id"); })
                                            .def_property("model_id", [](OpenBatch& s) { return get_string(s.model_id); },
                                                          [](OpenBatch& s, const std::string& v) { set_bounded_string(s.model_id, v, "model_id"); })
                                            .def_readwrite("size", &OpenBatch::size)
                                            .def_readwrite("create_ts_ms", &OpenBatch::create_ts_ms)
                                            .def("to_dict", [](OpenBatch& s) {
                                            py::dict d;
                                            d["batch_id"] = get_string(s.batch_id);
                                            d["model_id"] = get_string(s.model_id);
                                            d["size"] = s.size;
                                            d["create_ts_ms"] = s.create_ts_ms;
                                            return d;
                                                 });

                                        // Claim
                                        py::class_<Claim, ClaimHolder>(m, "Claim")
                                            .def(py::init([] {
                                            auto* p = new Claim();
                                            if (!data_structure::ClaimInitialize(p)) {
                                                delete p; throw std::runtime_error("ClaimInitialize failed");
                                            }
                                            return p;
                                                          }))
                                            .def_property("batch_id", [](Claim& s) { return get_string(s.batch_id); },
                                                          [](Claim& s, const std::string& v) { set_bounded_string(s.batch_id, v, "batch_id"); })
                                                              .def_property("worker_id", [](Claim& s) { return get_string(s.worker_id); },
                                                                            [](Claim& s, const std::string& v) { set_bounded_string(s.worker_id, v, "worker_id"); })
                                                              .def_readwrite("queue_length", &Claim::queue_length)
                                                              .def("to_dict", [](Claim& s) {
                                                              py::dict d;
                                                              d["batch_id"] = get_string(s.batch_id);
                                                              d["worker_id"] = get_string(s.worker_id);
                                                              d["queue_length"] = s.queue_length;
                                                              return d;
                                                                   });

                                                          // Grant
                                                          py::class_<Grant, GrantHolder>(m, "Grant")
                                                              .def(py::init([] {
                                                              auto* p = new Grant();
                                                              if (!data_structure::GrantInitialize(p)) {
                                                                  delete p; throw std::runtime_error("GrantInitialize failed");
                                                              }
                                                              return p;
                                                                            }))
                                                              .def_property("batch_id", [](Grant& s) { return get_string(s.batch_id); },
                                                                            [](Grant& s, const std::string& v) { set_bounded_string(s.batch_id, v, "batch_id"); })
                                                                                .def_property("winner_worker_id", [](Grant& s) { return get_string(s.winner_worker_id); },
                                                                                              [](Grant& s, const std::string& v) { set_bounded_string(s.winner_worker_id, v, "winner_worker_id"); })
                                                                                .def("to_dict", [](Grant& s) {
                                                                                py::dict d;
                                                                                d["batch_id"] = get_string(s.batch_id);
                                                                                d["winner_worker_id"] = get_string(s.winner_worker_id);
                                                                                return d;
                                                                                     });

                                                                            // TaskList
                                                                            py::class_<TaskList, TaskListHolder>(m, "TaskList")
                                                                                .def(py::init([] {
                                                                                auto* p = new TaskList();
                                                                                if (!data_structure::TaskListInitialize(p)) {
                                                                                    delete p; throw std::runtime_error("TaskListInitialize failed");
                                                                                }
                                                                                return p;
                                                                                              }))
                                                                                .def_property("batch_id", [](TaskList& s) { return get_string(s.batch_id); },
                                                                                              [](TaskList& s, const std::string& v) { set_bounded_string(s.batch_id, v, "batch_id"); })
                                                                                                  .def_property("model_id", [](TaskList& s) { return get_string(s.model_id); },
                                                                                                                [](TaskList& s, const std::string& v) { set_bounded_string(s.model_id, v, "model_id"); })
                                                                                                  .def_property("assigned_worker_id", [](TaskList& s) { return get_string(s.assigned_worker_id); },
                                                                                                                [](TaskList& s, const std::string& v) { set_bounded_string(s.assigned_worker_id, v, "assigned_worker_id"); })
                                                                                                  .def_property("tasks", [](TaskList& s) { return taskseq_to_list(s.tasks); },
                                                                                                                [](TaskList& s, py::handle obj) { list_to_taskseq(s.tasks, obj); })
                                                                                                  .def("to_dict", [](TaskList& s) {
                                                                                                  py::dict d;
                                                                                                  d["batch_id"] = get_string(s.batch_id);
                                                                                                  d["model_id"] = get_string(s.model_id);
                                                                                                  d["assigned_worker_id"] = get_string(s.assigned_worker_id);
                                                                                                  d["tasks"] = taskseq_to_list(s.tasks);
                                                                                                  return d;
                                                                                                       });

                                                                                              // WorkerTaskResult
                                                                                              py::class_<WorkerTaskResult, WorkerTaskResultHolder>(m, "WorkerTaskResult")
                                                                                                  .def(py::init([] {
                                                                                                  auto* p = new WorkerTaskResult();
                                                                                                  if (!data_structure::WorkerTaskResultInitialize(p)) {
                                                                                                      delete p; throw std::runtime_error("WorkerTaskResultInitialize failed");
                                                                                                  }
                                                                                                  return p;
                                                                                                                }))
                                                                                                  .def_property("request_id", [](WorkerTaskResult& s) { return get_string(s.request_id); },
                                                                                                                [](WorkerTaskResult& s, const std::string& v) { set_bounded_string(s.request_id, v, "request_id"); })
                                                                                                                    .def_property("task_id", [](WorkerTaskResult& s) { return get_string(s.task_id); },
                                                                                                                                  [](WorkerTaskResult& s, const std::string& v) { set_bounded_string(s.task_id, v, "task_id"); })
                                                                                                                    .def_property("client_id", [](WorkerTaskResult& s) { return get_string(s.client_id); },
                                                                                                                                  [](WorkerTaskResult& s, const std::string& v) { set_bounded_string(s.client_id, v, "client_id"); })
                                                                                                                    .def_property("status", [](WorkerTaskResult& s) { return get_string(s.status); },
                                                                                                                                  [](WorkerTaskResult& s, const std::string& v) { set_bounded_string(s.status, v, "status"); })
                                                                                                                    .def_property("output_blob", [](WorkerTaskResult& s) { return bytes_to_py(s.output_blob); },
                                                                                                                                  [](WorkerTaskResult& s, py::handle obj) { py_to_bytes(s.output_blob, obj); })
                                                                                                                    .def("to_dict", [](WorkerTaskResult& s) {
                                                                                                                    py::dict d;
                                                                                                                    d["request_id"] = get_string(s.request_id);
                                                                                                                    d["task_id"] = get_string(s.task_id);
                                                                                                                    d["client_id"] = get_string(s.client_id);
                                                                                                                    d["status"] = get_string(s.status);
                                                                                                                    d["output_blob"] = bytes_to_py(s.output_blob);
                                                                                                                    return d;
                                                                                                                         });

                                                                                                                // WorkerResult
                                                                                                                py::class_<WorkerResult, WorkerResultHolder>(m, "WorkerResult")
                                                                                                                    .def(py::init([] {
                                                                                                                    auto* p = new WorkerResult();
                                                                                                                    if (!data_structure::WorkerResultInitialize(p)) {
                                                                                                                        delete p; throw std::runtime_error("WorkerResultInitialize failed");
                                                                                                                    }
                                                                                                                    return p;
                                                                                                                                  }))
                                                                                                                    .def_property("batch_id", [](WorkerResult& s) { return get_string(s.batch_id); },
                                                                                                                                  [](WorkerResult& s, const std::string& v) { set_bounded_string(s.batch_id, v, "batch_id"); })
                                                                                                                                      .def_property("model_id", [](WorkerResult& s) { return get_string(s.model_id); },
                                                                                                                                                    [](WorkerResult& s, const std::string& v) { set_bounded_string(s.model_id, v, "model_id"); })
                                                                                                                                      .def_property("worker_id", [](WorkerResult& s) { return get_string(s.worker_id); },
                                                                                                                                                    [](WorkerResult& s, const std::string& v) { set_bounded_string(s.worker_id, v, "worker_id"); })
                                                                                                                                      .def_property("results", [](WorkerResult& s) { return wtrseq_to_list(s.results); },
                                                                                                                                                    [](WorkerResult& s, py::handle obj) { list_to_wtrseq(s.results, obj); })
                                                                                                                                      .def("to_dict", [](WorkerResult& s) {
                                                                                                                                      py::dict d;
                                                                                                                                      d["batch_id"] = get_string(s.batch_id);
                                                                                                                                      d["model_id"] = get_string(s.model_id);
                                                                                                                                      d["worker_id"] = get_string(s.worker_id);
                                                                                                                                      d["results"] = wtrseq_to_list(s.results);
                                                                                                                                      return d;
                                                                                                                                           });
}
