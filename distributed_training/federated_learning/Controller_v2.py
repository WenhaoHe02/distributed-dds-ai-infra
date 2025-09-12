# ...existing code...

import DDS_All as dds
import struct
import time
import json
import os
import numpy as np
import pickle

# Controller_v2: 支持 INT8 稀疏(sparse)与 dense(Q8) 以及 FP32 回退的聚合
# 运行：python Controller_v2.py controller_v2.conf.json

class Config:
	def __init__(self, jo):
		self.domain_id = jo["domain_id"]
		self.expected_clients = jo["expected_clients"]
		self.timeout_ms = jo["timeout_ms"]
		self.rounds = jo.get("rounds", 1)
		self.python_exe = jo["python_exe"]
		self.eval_script = jo["eval_script"]
		self.data_dir = jo["data_dir"]
		self.batch_size = jo.get("batch_size", 64)
		print(f"[Controller_v2] cfg: domain={self.domain_id} expected_clients={self.expected_clients} timeout_ms={self.timeout_ms}")

class Controller_v2:
	TOPIC_TRAIN_CMD = "train_scripts/train_cmd"
	TOPIC_CLIENT_UPDATE = "train_scripts/client_update"
	TOPIC_MODEL_BLOB = "train_scripts/model_blob"

	def __init__(self, config):
		self.config = config
		self.updates_map = {}
		self.round_counter = 1
		self.dp = None
		self.publisher = None
		self.subscriber = None
		self.trainCmdWriter = None
		self.modelBlobWriter = None
		self.clientUpdateReader = None
		self.listener = None
		# 动态加载评估模块
		self.eval_module = self.load_eval_module(config.eval_script)

	# 注意这里要加 script_path 参数
	def load_eval_module(self, script_path):
		import importlib.util
		import sys
		import os

		# 1. 把脚本所在目录加入 sys.path
		script_dir = os.path.dirname(script_path)
		if script_dir not in sys.path:
			sys.path.insert(0, script_dir)

		# 2. 动态加载模块
		spec = importlib.util.spec_from_file_location("eval_module", script_path)
		eval_module = importlib.util.module_from_spec(spec)
		sys.modules["eval_module"] = eval_module
		spec.loader.exec_module(eval_module)
		return eval_module   
		
	class ClientUpdateListener(dds.DataReaderListener):
		def __init__(self, ctrl):
			super().__init__()
			self.ctrl = ctrl
		def on_data_available(self, reader):
			clientUpdateSeq = dds.ClientUpdateSeq()
			sampleInfoSeq = dds.SampleInfoSeq()
			rtn = reader.take(clientUpdateSeq, sampleInfoSeq, 10, dds.ANY_SAMPLE_STATE, dds.ANY_VIEW_STATE, dds.ANY_INSTANCE_STATE)
			if rtn != dds.OK:
				print("take failed")
				return
			for i in range(clientUpdateSeq.length()):
				cu = clientUpdateSeq.get_at(i)
				info = sampleInfoSeq.get_at(i)
				if not info.valid_data:
					continue
				round_id = cu.round_id
				if round_id not in self.ctrl.updates_map:
					self.ctrl.updates_map[round_id] = []
				self.ctrl.updates_map[round_id].append(cu)

				print(f"[Controller_v2] recv update: client={cu.client_id} round={cu.round_id} bytes={len(cu.data)} nsamples={cu.num_samples}")
			reader.return_loan(clientUpdateSeq, sampleInfoSeq)

	@staticmethod
	def wait_writer_matched(writer, min_matches, timeout_ms):
		"""
		等待 DataWriter 至少匹配到 min_matches 个 DataReader，并打印状态
		:param writer: DDS DataWriter 对象
		:param min_matches: 最小匹配数
		:param timeout_ms: 超时时间（毫秒）
		:return: bool 是否匹配成功
		"""
		start = time.time() * 1000  # 毫秒
		last = -1

		while (time.time() * 1000 - start) < timeout_ms:
			st = writer.get_publication_matched_status()

			if st.current_count != last:
				print(f"[Controller_v2] Writer matched: current={st.current_count} "
					f"total={st.total_count} change={st.current_count_change}")
				last = st.current_count

			if st.current_count >= min_matches:
				return True

			time.sleep(0.1)  # 100 ms

		return False

	@staticmethod
	def wait_reader_matched(reader, min_matches, timeout_ms):
		"""
		等待 DataReader 至少匹配到 min_matches 个 DataWriter，并打印状态
		:param reader: DDS DataReader 对象
		:param min_matches: 最小匹配数
		:param timeout_ms: 超时时间（毫秒）
		:return: bool 是否匹配成功
		"""
		start = time.time() * 1000  # 毫秒
		last = -1
		
		while (time.time() * 1000 - start) < timeout_ms:
			st = reader.get_subscription_matched_status()
			
			if st.current_count != last:
				print(f"[Controller_v2] Reader matched: current={st.current_count} "
					f"total={st.total_count} change={st.current_count_change}")
				last = st.current_count

			if st.current_count >= min_matches:
				return True

			time.sleep(0.1)  # 100 ms

		return False

	def init_dds(self):
		factory = dds.DomainParticipantFactory.get_instance()
		self.dp = factory.create_participant(self.config.domain_id, dds.DOMAINPARTICIPANT_QOS_DEFAULT, None, 0)
		if self.dp is None:
			raise RuntimeError("create participant failed")
		dds.register_all_types(self.dp)

		trainCmdTopic = self.dp.create_topic(self.TOPIC_TRAIN_CMD, "TrainCmd", dds.TOPIC_QOS_DEFAULT, None, 0)
		clientUpdateTopic = self.dp.create_topic(self.TOPIC_CLIENT_UPDATE, "ClientUpdate", dds.TOPIC_QOS_DEFAULT, None, 0)
		modelBlobTopic = self.dp.create_topic(self.TOPIC_MODEL_BLOB, "ModelBlob", dds.TOPIC_QOS_DEFAULT, None, 0)
		self.publisher = self.dp.create_publisher(dds.PUBLISHER_QOS_DEFAULT, None, 0)
		self.subscriber = self.dp.create_subscriber(dds.SUBSCRIBER_QOS_DEFAULT, None, 0)

		# 写者QoS
		wq = dds.DataWriterQos()
		self.publisher.get_default_datawriter_qos(wq)
		wq.reliability.kind = dds.RELIABLE_RELIABILITY_QOS
		wq.history.kind = dds.KEEP_LAST_HISTORY_QOS
		wq.history.depth = 2

        # TrainCmd 写者
		self.trainCmdWriter = self.publisher.create_datawriter(trainCmdTopic, wq, None, 0)
		if self.trainCmdWriter is None:
			raise RuntimeError("create TrainCmd DataWriter failed")
		print("[Controller_v2] TrainCmd writer created")
		w1 = self.wait_writer_matched(self.trainCmdWriter, 1, 5000)
		print("[Controller_v2] TrainCmd writer matched=" + str(w1))

        # ModelBlob 写者
		self.modelBlobWriter = self.publisher.create_datawriter(modelBlobTopic, wq, None, 0)
		if self.modelBlobWriter is None:
			raise RuntimeError("create ModelBlob DataWriter failed")
		print("[Controller_v2] ModelBlob writer created")
		w2 = self.wait_writer_matched(self.trainCmdWriter, 1, 5000)
		print("[Controller_v2] ModelBlob writer matched=" + str(w2))

        # 读者Qos
		rq = dds.DataReaderQos()
		self.subscriber.get_default_datareader_qos(rq)
		rq.reliability.kind = dds.RELIABLE_RELIABILITY_QOS
		rq.history.kind = dds.KEEP_LAST_HISTORY_QOS
		rq.history.depth = 2

        # ModelBlob 读者
		self.listener = Controller_v2.ClientUpdateListener(self) 
		self.clientUpdateReader = self.subscriber.create_datareader(clientUpdateTopic, rq, self.listener, 1024)
		if self.clientUpdateReader is None:
			print("[Controller_v2] ClientUpdate reader is None")
		print("[Controller_v2] ClientUpdate reader created")
		r1 = self.wait_reader_matched(self.clientUpdateReader, 1, 5000)
		print("[Controller_v2] ClientUpdate reader matched=" + str(r1))

	def run_training_round(self, subset_size, epochs, lr, seed):
		round_id = self.round_counter
		self.round_counter += 1
		print(f"[Controller_v2] start round {round_id}")
		t_start = time.time()
		
		# 再次确认 TrainCmd writer 已有订阅者（跨机时发现可能更慢）
		ready = self.wait_writer_matched(self.trainCmdWriter, 1, 3000)
		print("[Controller_v2] Before send TrainCmd matched=" + str(ready))

		# 发送TrainCmd
		cmd = dds.TrainCmd()
		cmd.round_id = round_id
		cmd.subset_size = int(subset_size)
		cmd.epochs = epochs
		cmd.lr = lr
		cmd.seed = seed
		rc=self.trainCmdWriter.write(cmd)
		if(rc!=dds.OK):
			print(f"[Controller_v2] write TrainCmd failed:{rc} ")
			return
		else:
			print(f"[Controller_v2] TrainCmd written OK (round= {round_id} )")


		# 等待客户端更新
		collected = self.wait_for_client_updates(round_id, self.config.expected_clients, self.config.timeout_ms)
		if not collected:
			print("[Controller_v2] no updates collected, skipping aggregation for this round")
			return
		print(f"[Controller_v2] collected {len(collected)} updates (expected {self.config.expected_clients})")

		# 聚合
		aggregated = self.aggregate_fedavg_auto(collected)
		model_data = self.float32_to_bytes_le(aggregated)

		# 发布聚合模型
		blob = dds.ModelBlob()
		blob.round_id = round_id
		blob.data = model_data
		rc=self.modelBlobWriter.write(blob)
		if(rc!=dds.OK):
			print(f"[Controller_v2] write ModelBlob failed:{rc}")
		else:
			print(f"[Controller_v2] published model, bytes={len(model_data)}")

		t_end = time.time()
		print(f"[Controller_v2] e2e time: {int((t_end-t_start)*1000)} ms")

		# 评估
		self.evaluate_model(model_data)
		self.updates_map.pop(round_id, None)

	def wait_for_client_updates(self, round_id, expected_clients, timeout_ms):
		start = time.time()
		last = -1
		while (time.time() - start) * 1000 < timeout_ms:
			lst = self.updates_map.get(round_id, [])
			cnt = len(lst)
			if cnt != last:
				print(f"[Controller_v2] progress {cnt}/{expected_clients}")
				last = cnt
			if cnt >= expected_clients:
				break
			time.sleep(0.1)
		lst = self.updates_map.get(round_id, [])
		if not lst:
			print(f"[Controller_v2] WARNING: No updates received within timeout {timeout_ms} ms")
			return []
		if len(lst) < expected_clients:
			print(f"[Controller_v2] WARNING: Timeout reached ({timeout_ms} ms), only {len(lst)}/{expected_clients} clients responded. Proceeding with partial aggregation.")
		return list(lst)

	def aggregate_fedavg_auto(self, updates):
		if not updates:
			return np.array([], dtype=np.float32)
		vecs = []
		ws = []
		dim = -1
		for cu in updates:
			payload = cu.data
			v = None
			if self.is_s8_sparse(payload):
				v = self.decode_s8_sparse_to_float(payload)
			elif self.is_q8_dense(payload):
				v = self.decode_q8_to_float(payload)
			else:
				v = self.bytes_to_float32_le(payload)
			if dim < 0:
				dim = len(v)
			if len(v) != dim:
				raise Exception("inconsistent dim among clients")
			vecs.append(v)
			ws.append(cu.num_samples)
		total_w = sum([max(0, w) for w in ws])
		if total_w <= 0:
			total_w = len(updates)
		out = np.zeros(dim, dtype=np.float32)
		for v, w in zip(vecs, ws):
			coef = float(w if w > 0 else 1) / total_w
			out += np.array(v, dtype=np.float32) * coef
		return out

	def is_s8_sparse(self, data):
		return len(data) >= 4 and data[0] == ord('S') and data[1] == ord('8') and data[2] == 0 and data[3] == 1

	def decode_s8_sparse_to_float(self, blob):
		bb = memoryview(blob)
		m0, m1, m2, ver = bb[0], bb[1], bb[2], bb[3]
		if m0 != ord('S') or m1 != ord('8') or m2 != 0 or ver != 1:
			raise Exception("bad S8 header")
		dim = int.from_bytes(bb[4:8], 'little')
		k = int.from_bytes(bb[8:12], 'little')
		scale = struct.unpack('<f', bb[12:16])[0]
		out = np.zeros(dim, dtype=np.float32)
		idx = [int.from_bytes(bb[16+i*4:16+(i+1)*4], 'little') for i in range(k)]
		for i in range(k):
			q = struct.unpack('b', bb[16+4*k+i:16+4*k+i+1])[0]
			id = idx[i]
			if id < 0 or id >= dim:
				continue
			out[id] += q * scale
		return out

	def is_q8_dense(self, data):
		return len(data) >= 4 and data[0] == ord('Q') and data[1] == ord('8') and data[2] == 0 and data[3] == 1

	def decode_q8_to_float(self, blob):
		bb = memoryview(blob)
		m0, m1, m2, ver = bb[0], bb[1], bb[2], bb[3]
		if m0 != ord('Q') or m1 != ord('8') or m2 != 0 or ver != 1:
			raise Exception("bad Q8 header")
		chunk = int.from_bytes(bb[4:8], 'little')
		totalL = int.from_bytes(bb[8:16], 'little')
		nChunks = int.from_bytes(bb[16:20], 'little')
		if totalL > 2**31-1:
			raise Exception("vector too large")
		total = totalL
		scales = [struct.unpack('<f', bb[20+4*i:20+4*(i+1)])[0] for i in range(nChunks)]
		out = np.zeros(total, dtype=np.float32)
		offset = 20 + 4*nChunks
		for ci in range(nChunks):
			s = ci*chunk
			e = min(s+chunk, total)
			sc = scales[ci]
			for j in range(s, e):
				q = struct.unpack('b', bb[offset:offset+1])[0]
				out[j] = q * sc
				offset += 1
		return out

	def bytes_to_float32_le(self, data):
		if len(data) % 4 != 0:
			raise Exception("fp32 bytes length not multiple of 4")
		n = len(data) // 4
		out = np.zeros(n, dtype=np.float32)
		for i in range(n):
			out[i] = struct.unpack('<f', data[i*4:(i+1)*4])[0]
		return out

	def float32_to_bytes_le(self, v):
		return b''.join([struct.pack('<f', float(x)) for x in v])

	def evaluate_model(self, model_data: bytes):
		print(f"[Controller_v2] Evaluating model, bytes={len(model_data)}")
		t0 = time.time()
		try:
			# 直接函数调用，不再 subprocess
			acc = self.eval_module.evaluate_from_bytes(
				model_bytes=model_data,
				data_dir=self.config.data_dir,
				batch_size=self.config.batch_size
			)
			t1 = time.time()
			print(f"[Controller_v2] Eval done: Accuracy={acc:.4f}, Time={int((t1-t0)*1000)} ms")
			return acc
		except Exception as e:
			print(f"[Controller_v2] Eval error: {e}")

	def parse_accuracy(self, output):
		try:
			i = output.find("Accuracy:")
			if i >= 0:
				j = i + len("Accuracy:")
				while j < len(output) and output[j].isspace():
					j += 1
				k = j
				while k < len(output) and (output[k].isdigit() or output[k] == '.'):
					k += 1
				return float(output[j:k])
		except Exception:
			pass
		return -1.0

def load_config(conf_path):
	with open(conf_path, 'r', encoding='utf-8') as f:
		jo = json.load(f)
	return Config(jo)

def main():
	import sys
	if len(sys.argv) != 2:
		print("Usage: python Controller_v2.py <controller_v2.conf.json>")
		return
	config = load_config(sys.argv[1])
	ctrl = Controller_v2(config)
	ctrl.init_dds()
	for i in range(config.rounds):
		ctrl.run_training_round(6000, 5, 0.01, 12345 + i)

if __name__ == "__main__":
	main()
