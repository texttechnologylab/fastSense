import sqlite3
import gzip
import multiprocessing as mp
import os
from typing import List, Dict

from ..data import DataDescriptor, ExampleWriter
from ..token import Token
from .utils import normalize_section_title


class ExampleExporter:

	@staticmethod
	def run(
			db_path: str,
			tokens_path: str,
			file_count: int,
			output_path: str,
			data_descriptors: Dict[str, DataDescriptor],
			subset_names: Dict[int, str]
	):
		os.makedirs(output_path, exist_ok=True)

		in_conn = sqlite3.connect(db_path)
		c_in = in_conn.cursor()

		possible_senses = {}
		for row in c_in.execute("select sense_group, id from sense_group_senses order by sense_group asc, id asc"):
			sense_group, sense = row
			possible_senses[sense_group] = possible_senses.get(sense_group, []) + [sense]

		out_db_path = os.path.join(output_path, "additional_data.sqlite3")
		if os.path.exists(out_db_path):
			print("Not writing 'additional_data.sqlite3' because file already exists!")
		else:
			out_conn = sqlite3.connect(out_db_path)
			c_out = out_conn.cursor()

			c_out.execute("""
			create table "senses" (
				"id" INTEGER PRIMARY KEY,
				"url" TEXT
			)
			""")

			c_out.execute("""
			create table "group_titles" (
				"id" INTEGER,
				"title" TEXT
			)
			""")

			c_out.execute("""
			create table "possible_senses" (
				"group_id" INTEGER,
				"sense_id" INTEGER
			)
			""")

			result_cursor = c_in.execute("select id, group_title from sense_groups")
			c_out.executemany("insert into group_titles values (?, ?)", result_cursor)

			result_cursor = c_in.execute("""
			select
				G.id,
				alternative_group_title
			from
				sense_groups G,
				alternative_group_titles A
			where
				G.group_title = A.group_title
			""")
			c_out.executemany("insert into group_titles values (?, ?)", result_cursor)

			query = """
			select SGS.id, A.title, X.title
			from sense_group_senses SGS, senses S
			left join articles A on S.article_id = A.id
			left join sections X on X.article_id = S.article_id and X.section_index = S.section_index
			where SGS.sense = S.id
			"""

			for row in c_in.execute(query):
				label, article_title, article_section = row

				article_url = "https://en.wikipedia.org/wiki/" + article_title
				if article_section is not None:
					article_url += "#" + normalize_section_title(article_section)

				c_out.execute("insert into senses values (?,?)", (label, article_url))

			for group_id, possible_senses_for_group in possible_senses.items():
				values = zip([group_id] * len(possible_senses_for_group), possible_senses_for_group)
				c_out.executemany("insert into possible_senses values (?,?)", values)

			out_conn.commit()
			out_conn.close()

		in_conn.close()

		writer_processes = []
		writer_queues_paragraphs = {}
		writer_queues_sentences = {}

		for name in data_descriptors.keys():
			data_output_path = os.path.join(output_path, name)
			os.makedirs(data_output_path, exist_ok=False)

		for subset_index, subset_name in subset_names.items():
			subset_queues_paragraphs = []
			subset_queues_sentences = []

			for name, data_descriptor in data_descriptors.items():
				queue = mp.Queue(20000)

				if data_descriptor.uses_sentences:
					subset_queues_sentences.append(queue)
				else:
					subset_queues_paragraphs.append(queue)

				data_output_path = os.path.join(output_path, name)
				data_descriptor.save(os.path.join(data_output_path, "data_descriptor.json"))

				writer_process = mp.Process(
					target=ExampleExporter.writer_task,
					args=(
						os.path.join(output_path, name, subset_name),
						subset_name,
						data_descriptor,
						queue
					)
				)
				writer_process.start()
				writer_processes.append(writer_process)

				print("Writer PID", writer_process.pid)

			if len(subset_queues_paragraphs) > 0:
				writer_queues_paragraphs[subset_index] = subset_queues_paragraphs

			if len(subset_queues_sentences) > 0:
				writer_queues_sentences[subset_index] = subset_queues_sentences

		reader_and_worker_processes = []
		paragraph_queues = []
		db_queues = []

		for i in range(file_count):
			paragraph_queue = mp.Queue(20000)
			paragraph_queues.append(paragraph_queue)

			db_queue = mp.Queue(10000)
			db_queues.append(db_queue)

			# files only contain tokens for articles with article_id % file_count == i
			path = os.path.join(tokens_path, "tokens_{:d}.gz".format(i))

			reader_process = mp.Process(target=ExampleExporter.data_reader_task, args=(path, paragraph_queue))
			reader_process.start()
			reader_and_worker_processes.append(reader_process)

			print("Reader PID", reader_process.pid)

			worker_process = mp.Process(
				target=ExampleExporter.worker_task,
				args=(
					db_queue,
					paragraph_queue,
					writer_queues_paragraphs,
					writer_queues_sentences,
					possible_senses
				)
			)
			worker_process.start()
			reader_and_worker_processes.append(worker_process)

			print("Worker PID", worker_process.pid)

		sql_process = mp.Process(target=ExampleExporter.db_task, args=(db_path, db_queues))
		sql_process.start()

		print("DB PID", sql_process.pid)

		for p in reader_and_worker_processes:
			p.join()

		for queues in list(writer_queues_paragraphs.values()) + list(writer_queues_sentences.values()):
			for q in queues:
				q.put(None)

		sql_process.join()

		for p in writer_processes:
			p.join()

	@staticmethod
	def writer_task(path: str, file_prefix: str, data_descriptor: DataDescriptor, input_queue: mp.Queue):
		def tuple_to_token(t):
			return Token(
				start=t[0],
				end=t[1],
				value=t[2],
				pos=t[3],
				lemma=t[4],
				before=t[5],
				after=t[6]
			)

		writer = ExampleWriter(
			path=path,
			file_prefix=file_prefix,
			data_descriptor=data_descriptor,
			number_of_workers=6
		)

		with writer:
			while True:
				example = input_queue.get()
				if example is None:
					break

				tokens, possible_senses_for_example, sense_group_sense_id = example

				if len(tokens) == 0 or len(possible_senses_for_example) == 0:
					print("Skipped empty example:", example)
					continue

				tokens = list(map(tuple_to_token, tokens))

				writer.write(tokens, possible_senses_for_example, sense_group_sense_id)

	@staticmethod
	def worker_task(
			db_queue: mp.Queue,
			paragraph_queue: mp.Queue,
			paragraph_writer_queues: Dict[int, List[mp.Queue]],
			sentence_writer_queues: Dict[int, List[mp.Queue]],
			possible_senses: Dict[int, List[int]]
	):
		buffer = {}
		max_paragraph_key_in_buffer = (-1, -1, -1)
		buffer_complete = False

		while True:
			paragraph = paragraph_queue.get()
			if paragraph is None:
				break

			p_key, p_sentences = paragraph

			if not buffer_complete:
				while max_paragraph_key_in_buffer <= p_key:
					example_info = db_queue.get()
					if example_info is None:
						buffer_complete = True
						break

					# (article_id, section_index, paragraph_index, sentence_index, group_id, sense_group_sense_id, dataset)

					paragraph_key = example_info[0:3]  # (article_id, section_index, paragraph_index)

					if paragraph_key not in buffer:
						buffer[paragraph_key] = []

					buffer[paragraph_key].append(example_info[3:7])
					max_paragraph_key_in_buffer = paragraph_key

			example_info_list = buffer.get(p_key, None)
			if example_info_list is None:
				continue

			all_tokens = []
			for sentence in p_sentences:
				all_tokens.extend(sentence)

			for example_info in example_info_list:
				sentence_index, group_id, sense_group_sense_id, dataset = example_info

				possible_senses_for_group = possible_senses[group_id]

				for q in paragraph_writer_queues.get(dataset, []):
					q.put((all_tokens, possible_senses_for_group, sense_group_sense_id))

				if sentence_index is None:
					for sentence in p_sentences:
						for q in sentence_writer_queues.get(dataset, []):
							q.put((sentence, possible_senses_for_group, sense_group_sense_id))
				else:
					for q in sentence_writer_queues.get(dataset, []):
						q.put((p_sentences[sentence_index], possible_senses_for_group, sense_group_sense_id))

			del buffer[p_key]

		if len(buffer) > 0:
			print("Did not find {:d} paragraphs!".format(len(buffer)))
			for p_key, example_info in buffer.items():
				print(p_key, example_info)

	@staticmethod
	def db_task(db_path: str, output_queues: List[mp.Queue]):
		queue_count = len(output_queues)

		conn = sqlite3.connect(db_path)
		c = conn.cursor()

		c.execute("select count(*) from data")
		total_paragraph_count = c.fetchone()[0]

		sql = """
		select
			D.article_id,
			D.section_index,
			D.paragraph_index,
			D.sentence_index,
			SGS.sense_group,
			D.sense_group_sense_id,
			D.dataset
		from
			data D,
			sense_group_senses SGS
		where
			D.sense_group_sense_id = SGS.id
		order by
			D.article_id asc,
			D.section_index asc,
			D.paragraph_index asc
		"""

		count = 0
		for row in c.execute(sql):
			output_queues[row[0] % queue_count].put(row)

			count += 1

			if count % 100 == 1:
				print("Progress: {:.6f}% ({:,d}/{:,d})".format(
					count / total_paragraph_count * 100.0,
					count,
					total_paragraph_count
				), end="\r")

		conn.close()

		for q in output_queues:
			q.put(None)

		print("Waiting for workers to finish...")

	@staticmethod
	def data_reader_task(tokens_path: str, output_queue: mp.Queue):
		with gzip.open(tokens_path, "rt", encoding="utf-8") as f:
			current_paragraph_key = None
			current_sentence_index = None
			sentences = []
			sentence_tokens = []

			for line in f:
				line = line[:-1]  # Remove \n from end of line
				line = line.split("\t")

				paragraph_key = tuple(map(int, line[0:3]))  # (article_id, section_index, paragraph_index)
				sentence_index, begin, end = map(int, line[3:6])
				original_text, pos, lemma, before, after = line[6:]

				if paragraph_key != current_paragraph_key:
					assert sentence_index == 0  # Index of next sentence

					if current_paragraph_key is not None and len(sentences) > 0:
						output_queue.put((current_paragraph_key, sentences))

					current_paragraph_key = paragraph_key
					current_sentence_index = 0

					sentences = []
					sentence_tokens = []
					sentences.append(sentence_tokens)
				elif sentence_index != current_sentence_index and current_sentence_index is not None:
					sentence_tokens = []
					sentences.append(sentence_tokens)
					current_sentence_index += 1

					assert sentence_index == current_sentence_index

				token = (begin, end, original_text, pos, lemma, before, after)
				sentence_tokens.append(token)

			if len(sentences) > 0:
				output_queue.put((current_paragraph_key, sentences))

		output_queue.put(None)

		print("[{}] Reading done!".format(tokens_path))
