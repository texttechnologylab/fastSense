from typing import List, Iterator
import tensorflow as tf
import os
import json
import multiprocessing as mp

from .token import Token


class DataDescriptor:
	"""
	Describes format of input data.
	"""

	PUNCTUATION = [".", ",", ";", ":", "?", "!", "\"", "'", "(", ")", "[", "]", "{", "}"]

	def __init__(
			self,
			n_gram_size: int,
			caseless: bool,
			ignore_punctuation: bool,
			add_pos_tags: bool,
			uses_lemma: bool,
			uses_sentences: bool
	):
		self.n_gram_size = n_gram_size
		self.caseless = caseless
		self.ignore_punctuation = ignore_punctuation
		self.add_pos_tags = add_pos_tags
		self.uses_lemma = uses_lemma
		self.uses_sentences = uses_sentences

	@staticmethod
	def build_n_grams(tokens: List[str], n_gram_size: int, min_n_gram_size: int = 1) -> List[str]:
		if n_gram_size == 1:
			return tokens

		results = []

		for i in range(min(n_gram_size - min_n_gram_size + 1, len(tokens))):
			current_n_gram_size = i + min_n_gram_size
			for j in range(max(len(tokens) - current_n_gram_size + 1, 1)):
				results.append("_".join(tokens[j:(j + current_n_gram_size)]))

		return results

	def prepare_tokens(self, tokens: Iterator[Token]) -> List[str]:
		"""
		Converts tokens into list of strings, based on format specifications.

		:param tokens: List of tokens.
		:return:  List of tokens as strings, including e.g. n-grams and transformations
		"""

		if self.ignore_punctuation:
			tokens = filter(lambda t: t.value not in self.PUNCTUATION, tokens)

		if self.uses_lemma:
			token_values = map(lambda t: t.lemma, tokens)
		else:
			token_values = map(lambda t: t.value, tokens)

		if self.caseless:
			token_values = map(lambda v: v.lower(), token_values)

		if self.add_pos_tags:
			token_values = map(lambda x: "_".join(x), zip(token_values, map(lambda t: t.pos, tokens)))

		n_gram_values = self.build_n_grams(tokens=list(token_values), n_gram_size=self.n_gram_size)

		return n_gram_values

	def save(self, path: str):
		"""
		Writes data descriptor to JSON file.

		:param path: Path including filename of output file.
		"""

		info_dict = {
			"n_gram_size": self.n_gram_size,
			"caseless": self.caseless,
			"ignore_punctuation": self.ignore_punctuation,
			"add_pos_tags": self.add_pos_tags,
			"uses_lemma": self.uses_lemma,
			"uses_sentences": self.uses_sentences
		}

		with open(path, "wt", encoding="utf8") as f:
			json.dump(info_dict, f)

	@staticmethod
	def load(path: str) -> "DataDescriptor":
		"""
		Loads data descriptor from JSON file.

		:param path: Path to JSON file.
		:return: DataDescriptor
		"""

		with open(path, "r") as f:
			info_dict = json.load(f)

		return DataDescriptor(
			n_gram_size=int(info_dict["n_gram_size"]),
			caseless=bool(info_dict["caseless"]),
			ignore_punctuation=bool(info_dict["ignore_punctuation"]),
			add_pos_tags=bool(info_dict["add_pos_tags"]),
			uses_lemma=bool(info_dict["uses_lemma"]),
			uses_sentences=bool(info_dict["uses_sentences"])
		)


class ExampleWriter:
	"""
	Writes examples to file on disk.
	"""

	MAX_EXAMPLES_PER_FILE = 3000000

	def __init__(self, path: str, file_prefix: str, data_descriptor: DataDescriptor, number_of_workers: int = 4):
		"""
		Creates writer.

		:param path: Path to output folder. May not already exist!
		:param file_prefix: Prefix for filenames.
		:param data_descriptor: Instance of `DataDescriptor`. Will be used to prepare tokens.
		:param number_of_workers: Number of processes used for preparing tokens.
		"""

		self.input_queue = mp.Queue(1000)
		self.serialized_examples_queue = mp.Queue(1000)

		self.example_count = 0

		self.writer_process = mp.Process(
			target=ExampleWriter._write_task,
			args=(
				path,
				file_prefix,
				self.serialized_examples_queue
			)
		)
		self.writer_process.start()

		worker_processes = []
		for _ in range(number_of_workers):
			worker_process = mp.Process(
				target=ExampleWriter._worker_task,
				args=(data_descriptor, self.input_queue, self.serialized_examples_queue)
			)
			worker_process.start()
			worker_processes.append(worker_process)

		self.worker_processes = worker_processes

	def close(self):
		"""
		Closes any open files. Call this after writing the last example or use a `with` statement.
		"""
		for _ in range(len(self.worker_processes)):
			self.input_queue.put(None)

		for p in self.worker_processes:
			p.join()

		self.serialized_examples_queue.put(None)
		self.writer_process.join()

	def write(self, tokens: List[Token], possible_senses: List[int], sense: int):
		"""
		Writes example.

		:param tokens: List of `Token` instances.
		:param possible_senses: List of possible senses. Sorted ascending.
		:param sense: Sense. Must also be in possible senses!
		:return:
		"""

		self.input_queue.put((tokens, possible_senses, sense))
		self.example_count += 1

	@staticmethod
	def _worker_task(data_descriptor: DataDescriptor, in_queue: mp.Queue, out_queue: mp.Queue):
		while True:
			write_task = in_queue.get()
			if write_task is None:
				break

			tokens, possible_senses, sense = write_task
			assert sense in possible_senses

			prepared_tokens = data_descriptor.prepare_tokens(tokens=tokens)
			if len(prepared_tokens) == 0:
				print("Skipped empty example:", write_task)
				continue

			encoded_tokens = list(map(lambda s: s.encode("utf8"), prepared_tokens))

			feature = {
				"tokens": tf.train.Feature(bytes_list=tf.train.BytesList(value=encoded_tokens)),
				"possible_senses": tf.train.Feature(int64_list=tf.train.Int64List(value=possible_senses)),
				"sense": tf.train.Feature(int64_list=tf.train.Int64List(value=[sense]))
			}

			example = tf.train.Example(features=tf.train.Features(feature=feature))
			serialized_example = example.SerializeToString()

			out_queue.put(serialized_example)

	@staticmethod
	def _write_task(path: str, file_prefix: str, queue: mp.Queue):
		os.makedirs(path, exist_ok=False)

		file_options = tf.python_io.TFRecordOptions(compression_type=tf.python_io.TFRecordCompressionType.GZIP)

		writer = None
		next_file_index = 0

		examples_in_current_file = 0

		while True:
			serialized_example = queue.get()
			if serialized_example is None:
				break

			if writer is None or examples_in_current_file >= ExampleWriter.MAX_EXAMPLES_PER_FILE:
				filename = file_prefix + "." + str(next_file_index).rjust(3, "0") + ".tfrecords.gz"
				next_file_index += 1

				if writer is not None:
					writer.close()

				writer = tf.python_io.TFRecordWriter(
					os.path.join(path, filename),
					options=file_options
				)
				examples_in_current_file = 0

			writer.write(serialized_example)

			examples_in_current_file += 1

		if writer is not None:
			writer.close()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()
		return False
