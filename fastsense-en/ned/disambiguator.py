from typing import List, Tuple, Optional
import os
import sqlite3
import tensorflow as tf
import re
from bisect import bisect_left
import multiprocessing as mp

from .token import Token
from .corenlp import CoreNlpBridge
from .data import DataDescriptor


class Disambiguator:
	"""
	Class for loading and using trained models for word sense disambiguation.
	"""

	def __init__(self, model_path: str, corenlp_bridge: Optional[CoreNlpBridge] = None, worker_count: int = None):
		"""
		Initializes disambiguator.

		:param model_path: Path to folder containing model
		:param corenlp_bridge: Instance of CoreNlpBridge
		:param worker_count: Number of parallel instances for disambiguating paragraphs. Keep in mind that workers do
			not share memory, including memory for the neural network!
		"""
		self.data_descriptor = DataDescriptor.load(os.path.join(model_path, "assets.extra", "data_descriptor.json"))

		db_path = os.path.join(model_path, "assets.extra", "senses.sqlite3")
		self.db_path = db_path

		db_conn = sqlite3.connect(db_path)
		c = db_conn.cursor()

		c.execute("""
			with groups as (select group_id from possible_senses group by group_id having count(*) > 1)
			select
				group_id,
				sense_id
			from
				possible_senses
			where
				group_id in groups
			order by
				group_id asc,
				sense_id asc
		""")

		possible_senses = {}

		for group_id, sense_id in c:
			possible_senses_for_group = possible_senses.get(group_id, None)
			if possible_senses_for_group is None:
				possible_senses_for_group = []
				possible_senses[group_id] = possible_senses_for_group

			possible_senses_for_group.append(sense_id)

		c.execute("""
			with groups as (select group_id from possible_senses group by group_id having count(*) > 1)
			select distinct
				id,
				title
			from
				group_titles
			where
				id in groups
			order by
				title asc
		""")

		sorted_ambiguous_phrases = []
		sorted_possible_senses = []

		for group_id, title in c.fetchall():
			sorted_ambiguous_phrases.append(title)
			sorted_possible_senses.append(possible_senses[group_id])

		del possible_senses

		db_conn.close()

		# if corenlp_bridge is None:
		# 	corenlp_bridge = CoreNlpBridge(
		# 		classpath="../stanford-corenlp/*",
		# 		properties={
		# 			"annotators": "tokenize,ssplit,pos,lemma",
		# 			"tokenize.options": "untokenizable=noneKeep,invertible=true,ptb3Escaping=false",
		# 			"tokenize.language": "en"
		# 		}
		# 	)

		self.corenlp_bridge = corenlp_bridge

		self.in_queue = mp.Queue()
		self.out_queue = mp.Queue()

		self.worker_processes = []

		if worker_count is None:
			worker_count = mp.cpu_count()

		for _ in range(worker_count):
			args = (
				self.in_queue,
				self.out_queue,
				sorted_ambiguous_phrases,
				sorted_possible_senses,
				self.data_descriptor,
				model_path,
				db_path
			)
			worker_process = mp.Process(target=self._disambig_task, args=args)
			worker_process.start()

			self.worker_processes.append(worker_process)

	def close(self):
		"""
		Closes CoreNLPBridge and TensorFlow session. Call this after you don't need the disambiguator anymore.
		"""

		if self.corenlp_bridge is not None:
			self.corenlp_bridge.close()
			self.corenlp_bridge = None

		for _ in range(len(self.worker_processes)):
			self.in_queue.put(None)

		for p in self.worker_processes:
			p.join()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()
		return False

	@staticmethod
	def _disambig_task(in_queue: mp.Queue, out_queue: mp.Queue, sorted_ambiguous_phrases: List[str], sorted_possible_senses: List[List[int]], data_descriptor: DataDescriptor, model_path: str, db_path: str):
		allowed_pos_tags = ["NN", "NNS", "NNP", "NNPS", "FW"]

		config = tf.ConfigProto()
		config.gpu_options.allow_growth = True

		db_conn = sqlite3.connect(db_path)
		c = db_conn.cursor()

		cache = {}

		with tf.Session(config=config) as session:
			metagraph = tf.saved_model.loader.load(session, [tf.saved_model.tag_constants.SERVING], model_path)
			signature_def = metagraph.signature_def[tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY]

			inputs = signature_def.inputs
			tokens_placeholder = tf.saved_model.utils.get_tensor_from_tensor_info(inputs["tokens"])
			possible_senses_placeholder = tf.saved_model.utils.get_tensor_from_tensor_info(inputs["possible_senses"])

			outputs = signature_def.outputs
			out_layer_logits_tensor = tf.saved_model.utils.get_tensor_from_tensor_info(outputs["out_layer_logits"])

			while True:
				job = in_queue.get()
				if job is None:
					break

				job_id, tokens = job

				ambiguous_ranges = []  # (ambiguous_phrase_start, ambiguous_phrase_end, ambiguous_phrase_possible_senses)

				for i in range(0, len(tokens)):
					first_token = tokens[i]

					ambiguous_phrase = first_token.value.lower()
					ambiguous_phrase_start = first_token.start
					ambiguous_phrase_end = first_token.end
					ambiguous_phrase_index = bisect_left(sorted_ambiguous_phrases, ambiguous_phrase)

					contains_allowed_tag = first_token.pos is None or first_token.pos in allowed_pos_tags

					j = i + 1

					while ambiguous_phrase_index < len(sorted_ambiguous_phrases):
						phrase_at_index = sorted_ambiguous_phrases[ambiguous_phrase_index]
						if not phrase_at_index.startswith(ambiguous_phrase):
							break

						if phrase_at_index == ambiguous_phrase and contains_allowed_tag:
							ambiguous_range = (
								ambiguous_phrase_start,
								ambiguous_phrase_end,
								sorted_possible_senses[ambiguous_phrase_index]
							)
							ambiguous_ranges.append(ambiguous_range)

						if j >= len(tokens):
							break

						next_token = tokens[j]
						j += 1

						ambiguous_phrase += next_token.before.lower() + next_token.value.lower()
						ambiguous_phrase_end = next_token.end
						contains_allowed_tag = contains_allowed_tag or (next_token.pos in allowed_pos_tags)

						ambiguous_phrase_index = bisect_left(sorted_ambiguous_phrases, ambiguous_phrase, lo=ambiguous_phrase_index)

				if len(ambiguous_ranges) == 0:
					out_queue.put((job_id, []))
					continue

				prepared_tokens = data_descriptor.prepare_tokens(tokens)
				prepared_tokens = list(map(lambda t: t.encode("utf-8"), prepared_tokens))

				segment_possible_senses = set()
				for _, _, ambiguous_phrase_possible_senses in ambiguous_ranges:
					for sense in ambiguous_phrase_possible_senses:
						segment_possible_senses.add(sense)

				segment_possible_senses = sorted(segment_possible_senses)

				feed_dict = {
					tokens_placeholder: prepared_tokens,
					possible_senses_placeholder: segment_possible_senses
				}

				out_layer_logits = session.run(out_layer_logits_tensor, feed_dict=feed_dict)

				sorted_logits = sorted(zip(out_layer_logits, segment_possible_senses), reverse=True)

				disambiguated_ranges = []

				for ambiguous_phrase_start, ambiguous_phrase_end, ambiguous_phrase_possible_senses in ambiguous_ranges:
					for value, sense in sorted_logits:
						index = bisect_left(ambiguous_phrase_possible_senses, sense)
						if index < len(ambiguous_phrase_possible_senses) and ambiguous_phrase_possible_senses[
							index] == sense:
							disambiguated_range = (
								ambiguous_phrase_start,
								ambiguous_phrase_end,
								sense
							)
							disambiguated_ranges.append(disambiguated_range)
							break

				output = []

				for start, end, sense in disambiguated_ranges:
					if sense in cache:
						article_url = cache[sense]
					else:
						c.execute("select url from senses where id = ?", (sense,))
						article_url = c.fetchone()[0]

						cache[sense] = article_url

					output.append((start, end, article_url))

				out_queue.put((job_id, output))

	def divide_and_tokenize(self, text: str) -> List[List[Token]]:
		"""
		Tokenizes text and splits it into paragraphs or sentences based on `self.data_descriptor.uses_sentences`.

		:param text: Input text
		:return: List containing segments. Each segment (paragraph or sentence) is a list containing tokens.
		"""

		assert self.corenlp_bridge is not None

		input_paragraphs = []  # [((start index, end index), text), ...]

		next_paragraph_start = 0
		for whitespace_between_paragraphs_match in re.finditer(r"(\s*\n\s*\n\s*)", text):
			paragraph_start = next_paragraph_start
			paragraph_end = whitespace_between_paragraphs_match.start()
			next_paragraph_start = whitespace_between_paragraphs_match.end()

			paragraph_text = text[paragraph_start:paragraph_end]

			if len(paragraph_text) > 0:
				input_paragraphs.append((paragraph_start, paragraph_text))

		last_paragraph = (next_paragraph_start, text[next_paragraph_start:])
		input_paragraphs.append(last_paragraph)

		tokenized_paragraphs = self.corenlp_bridge.tokenize(input_paragraphs)

		output = []

		for sentences in tokenized_paragraphs:

			if self.data_descriptor.uses_sentences:
				for sentence in sentences:
					output.append(sentence)
			else:
				paragraph_tokens = []
				for sentence in sentences:
					paragraph_tokens.extend(sentence)
				output.append(paragraph_tokens)

		return output

	def disambiguate_tokenized_segments(self, segments: List[List[Token]]) -> List[Tuple[int, int, str]]:
		"""
		Returns Wikipedia URLs for ambiguous words in tokenized text. Results may overlap.

		:param text: Input text
		:return: List containing tuples: (start index, end index, Wikipedia URL)
		"""

		if len(segments) == 0:
			return []

		for i in range(len(segments)):
			self.in_queue.put((i, segments[i]))

		segment_out_count = 0
		output = []

		while segment_out_count < len(segments):
			segment_index, disambiguated_ranges = self.out_queue.get()
			segment_out_count += 1

			output += disambiguated_ranges  # (start, end, article_url)

		return sorted(output, key=lambda x: x[0])

	def disambiguate(self, text: str) -> List[Tuple[int, int, str]]:
		"""
		Returns Wikipedia URLs for ambiguous words in text. Results may overlap.

		:param text: Input text
		:return: List containing tuples: (start index, end index, Wikipedia URL)
		"""

		if len(text) == 0:
			return []

		segments = self.divide_and_tokenize(text)
		return self.disambiguate_tokenized_segments(segments)
