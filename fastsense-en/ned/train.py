import os
import tensorflow as tf
from typing import Dict, List
import sqlite3
import json
import datetime
import glob

from ned.estimator import WordSenseEstimator, file_input_fn


class ModelTrainer:
	def __init__(self, model_dir: str, dataset_base_path: str, dataset_name: str, parameters: Dict[str, any]):
		self.dataset_path = os.path.join(dataset_base_path, dataset_name)
		self.db_path = os.path.join(dataset_base_path, "additional_data.sqlite3")
		self.data_descriptor_path = os.path.join(self.dataset_path, "data_descriptor.json")
		self.model_dir = model_dir

		if os.path.exists(self.data_descriptor_path) and os.path.exists(self.db_path):
			conn = sqlite3.connect(self.db_path)
			c = conn.execute("select count(*) from senses")
			number_of_senses = c.fetchone()[0]
			conn.close()
		else:
			print("Counting senses...")
			number_of_senses = 0

			file_options = tf.python_io.TFRecordOptions(compression_type=tf.python_io.TFRecordCompressionType.GZIP)
			file_pattern = os.path.join(self.dataset_path, "train", "train.*.tfrecords.gz")

			for train_file_path in glob.glob(file_pattern):
				for serializedExample in tf.python_io.tf_record_iterator(path=train_file_path, options=file_options):
					example = tf.train.Example()
					example.ParseFromString(serializedExample)
					sense = int(example.features.feature["sense"].int64_list.value[0])
					if sense >= number_of_senses:
						number_of_senses = sense + 1

			print("Counting senses done!")

		session_config = tf.ConfigProto()
		session_config.allow_soft_placement = False
		session_config.gpu_options.allow_growth = True

		config = tf.estimator.RunConfig(
			save_summary_steps=60000,
			save_checkpoints_secs=1800,
			log_step_count_steps=10000,
			keep_checkpoint_max=2,
			session_config=session_config
		)

		self.estimator = WordSenseEstimator(
			number_of_senses=number_of_senses,
			model_dir=model_dir,
			params=parameters,
			config=config
		)

	def train(self, dataset_name: str, epochs: int, batch_size: int):
		file_pattern = os.path.join(self.dataset_path, dataset_name, dataset_name + ".*.tfrecords.gz")

		def input_fn():
			return file_input_fn(file_pattern=file_pattern, epochs=epochs, batch_size=batch_size, shuffle=True)

		self.estimator.train(input_fn=input_fn)

	def test(self, dataset_name: str) -> dict:
		file_pattern = os.path.join(self.dataset_path, dataset_name, dataset_name + ".*.tfrecords.gz")

		def input_fn():
			return file_input_fn(file_pattern=file_pattern, epochs=1, batch_size=256, shuffle=False)

		results = self.estimator.evaluate(input_fn=input_fn, name=dataset_name)

		serializable_results = {}
		for key, value in results.items():
			if type(value).__module__ == "numpy":
				serializable_results[key] = value.tolist()
			else:
				serializable_results[key] = value

		return serializable_results

	def export(self, export_dir_base: str):
		serving_input_receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(
			features={
				"tokens": tf.placeholder(tf.string, [None]),
				"possible_senses": tf.placeholder(tf.int64, [None])
			}
		)
		assets_extra = {
			"senses.sqlite3": self.db_path,
			"data_descriptor.json": self.data_descriptor_path
		}

		return self.estimator.export_savedmodel(
			export_dir_base=export_dir_base,
			serving_input_receiver_fn=serving_input_receiver_fn,
			assets_extra=assets_extra
		)


class TrainJob:
	def __init__(self, dataset_name: str, model_name: str, params: dict, epochs: int, batch_size: int, train_sets: List[str], test_sets: List[str]):
		self.dataset_name = dataset_name
		self.model_name = model_name
		self.params = params
		self.epochs = epochs
		self.batch_size = batch_size
		self.train_sets = train_sets
		self.test_sets = test_sets

	def run(self, job_runner: "TrainJobRunner"):
		model_dir = os.path.join(job_runner.models_dir, self.model_name)
		trainer = ModelTrainer(model_dir, job_runner.dataset_base_path, self.dataset_name, self.params)

		if len(self.train_sets) > 0:
			train_duration = 0.0
			for train_set in self.train_sets:
				print(("\033[0;34m" + "[{}-{}]" + "\033[m" + " Starting training...").format(self.model_name, train_set))
				train_start_time = datetime.datetime.now()
				trainer.train(train_set, self.epochs, self.batch_size)
				train_duration += (datetime.datetime.now() - train_start_time).total_seconds()
		else:
			train_duration = None

		all_results = {}
		for test_set in self.test_sets:
			print(("\033[0;34m" + "[{}-{}]" + "\033[m" + " Starting test...").format(self.model_name, test_set))
			start_time = datetime.datetime.now()
			results = trainer.test(test_set)
			results["duration"] = (datetime.datetime.now() - start_time).total_seconds()

			all_results[test_set] = results

			print(("\033[0;34m" + "[{}-{}]" + "\033[m" + " results = {}").format(self.model_name, test_set, results))

		if job_runner.final_models_dir is not None:
			final_model_path = trainer.export(job_runner.final_models_dir)
			final_model_path = final_model_path.decode("utf8")

			info_dict = {
				"model_name": self.model_name,
				"dataset_name": self.dataset_name,
				"train_duration": train_duration,
				"params": self.params,
				"epochs": self.epochs,
				"batch_size": self.batch_size,
				"results": all_results
			}

			with open(os.path.join(final_model_path, "info.json"), "w") as f:
				json.dump(info_dict, f)


class AutoTrainJob:
	def __init__(self, dataset_name: str, model_name: str, params: dict, target: Dict[str, any], batch_size: int, train_sets: List[str], test_sets: List[str]):
		self.dataset_name = dataset_name
		self.model_name = model_name
		self.params = params

		self.metric_key = target["metric_key"]
		self.flip_sign_of_metric = target.get("flip_sign_of_metric", False)
		self.end_if_slope_less_than = target["end_if_slope_less_than"]
		self.epochs_to_avg_over = target.get("epochs_to_avg_over", 1)
		self.test_after_epochs = target["test_after_epochs"]

		assert self.epochs_to_avg_over >= 1

		self.batch_size = batch_size
		self.train_sets = train_sets
		self.test_sets = test_sets

	def run(self, job_runner: "TrainJobRunner"):
		model_dir = os.path.join(job_runner.models_dir, self.model_name)
		trainer = ModelTrainer(model_dir, job_runner.dataset_base_path, self.dataset_name, self.params)

		assert len(self.train_sets) > 0
		assert len(self.test_sets) > 0

		max_avg_slope = None

		metric_history = {}
		for test_set in self.test_sets:
			metric_history[test_set] = []

		while max_avg_slope is None or max_avg_slope >= self.end_if_slope_less_than:
			max_avg_slope = None

			train_duration = 0.0
			for train_set in self.train_sets:
				print(("\033[0;34m" + "[{}-{}]" + "\033[m" + " Starting training...").format(self.model_name, train_set))
				train_start_time = datetime.datetime.now()
				trainer.train(train_set, self.test_after_epochs, self.batch_size)
				train_duration += (datetime.datetime.now() - train_start_time).total_seconds()

			all_results = {}
			for test_set in self.test_sets:
				print(("\033[0;34m" + "[{}-{}]" + "\033[m" + " Starting test...").format(self.model_name, test_set))
				start_time = datetime.datetime.now()
				results = trainer.test(test_set)
				results["duration"] = (datetime.datetime.now() - start_time).total_seconds()

				all_results[test_set] = results

				old_results = metric_history[test_set]

				prev_avg = sum(old_results) / len(old_results) if len(old_results) > 0 else None

				new_result = results[self.metric_key] * (-1.0 if self.flip_sign_of_metric else 1.0)
				old_results.append(new_result)

				while len(old_results) > self.epochs_to_avg_over:
					old_results.pop(0)

				if prev_avg is not None:
					new_avg = sum(old_results) / len(old_results)
					delta = new_avg - prev_avg

					if max_avg_slope is None or max_avg_slope < delta:
						max_avg_slope = delta

				print(("\033[0;34m" + "[{}-{}]" + "\033[m" + " results = {}").format(self.model_name, test_set, results))

			if job_runner.final_models_dir is not None:
				final_model_path = trainer.export(job_runner.final_models_dir)
				final_model_path = final_model_path.decode("utf8")

				info_dict = {
					"model_name": self.model_name,
					"dataset_name": self.dataset_name,
					"train_duration": train_duration,
					"params": self.params,
					"epochs": self.test_after_epochs,
					"batch_size": self.batch_size,
					"results": all_results
				}

				with open(os.path.join(final_model_path, "info.json"), "w") as f:
					json.dump(info_dict, f)


class TrainJobRunner:
	"""
	Class for loading and running train and test jobs. Not intented for use outside of CLI script.
	"""

	def __init__(self, dataset_base_path: str, models_dir: str, final_models_dir: str):
		self.dataset_base_path = dataset_base_path
		self.models_dir = models_dir
		self.final_models_dir = final_models_dir

		self.jobs = []

	def add_job(self, job: TrainJob):
		self.jobs.append(job)

	def load_jobs(self, path: str):
		with open(path, "r") as f:
			jobs_json = json.load(f)

		for job_dict in jobs_json:
			target = job_dict.get("target", None)

			if target is not None:
				job = AutoTrainJob(
					dataset_name=job_dict["dataset_name"],
					model_name=job_dict["model_name"],
					params=job_dict["params"],
					target=target,
					batch_size=job_dict["batch_size"],
					train_sets=job_dict.get("train_sets", ["train"]),
					test_sets=job_dict.get("test_sets", ["dev", "test"])
				)
			else:
				job = TrainJob(
					dataset_name=job_dict["dataset_name"],
					model_name=job_dict["model_name"],
					params=job_dict["params"],
					epochs=job_dict["epochs"],
					batch_size=job_dict["batch_size"],
					train_sets=job_dict.get("train_sets", ["train"]),
					test_sets=job_dict.get("test_sets", ["dev", "test"])
				)

			self.add_job(job)

	def run(self):
		while len(self.jobs) > 0:
			job = self.jobs.pop(0)

			print("\033[0;34m" + "=>" + "\033[m" + " Starting job for model '{}'...".format(job.model_name))

			job.run(job_runner=self)

			print("\033[0;34m" + "=>" + "\033[m" + " Job for model '{}' done!".format(job.model_name))
