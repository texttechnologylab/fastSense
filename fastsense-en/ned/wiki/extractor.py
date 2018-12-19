import datetime
import multiprocessing as mp
import re
import time
import html
import mwparserfromhell as mwp

from .utils import normalize_page_title, normalize_section_title
from .reader import WikiDumpReader
from .parser import WikitextParser


class Page:
	def __init__(self, page_id, page_title):
		self.id = page_id
		self.title = page_title
		self.redirect_article_title = None
		self.redirect_section_id = None
		self.sections = []


class WikiExtractor:

	def __init__(self, dump_path: str, namespaces: set = {0}):
		"""
		:param dump_path: Path to *-pages-articles.xml.bz2
		:param namespaces: Set of allowed namespaces
		"""
		self.dump_path = dump_path
		self.namespaces = namespaces

	@staticmethod
	def print_progress(progress: mp.Value, is_done: mp.Value):
		while is_done.value == 0:
			print("Read Progress: {:7.4f}%".format(progress.value * 100.0), end="\r")
			time.sleep(1)

		print("")
		print("Done!")

	def read_pages(self, page_queue: mp.Queue, number_of_workers: int, progress: mp.Value):
		with WikiDumpReader(self.dump_path) as wiki_reader:
			while True:
				page = wiki_reader.next_page()
				if page is None:
					break

				progress.value = wiki_reader.bytes_read / wiki_reader.total_bytes

				if page.ns not in self.namespaces:
					continue  # Only Main/Article namespace is interesting

				assert (len(page.revisions) > 0)

				if page.ns == 0 and (page.revisions[0].model != "wikitext" or page.revisions[0].format != "text/x-wiki"):
					print("Page " + str(page.id) + " (" + str(page.title) + ") not wikitext. (model=" + str(page.revisions[0].model) + "; format=" + page.revisions[0].format + ")")
					continue

				page_id = page.id
				page_title = page.title
				page_text = page.revisions[0].text.text

				redirect_match = re.match(r"^[ ]*?#REDIRECT[ ]*?\[\[(.*?)\]\]", page_text, re.IGNORECASE)
				if redirect_match is not None:
					redirect_link = redirect_match.group(1)
					redirect_link = html.unescape(redirect_link)

					link_components = redirect_link.split("#", 1)

					redirect_article_title = link_components[0]
					redirect_article_title = normalize_page_title(redirect_article_title)

					if len(link_components) > 1:
						redirect_section_id = link_components[1]
						redirect_section_id = normalize_section_title(redirect_section_id)
					else:
						redirect_section_id = None
				else:
					redirect_article_title = None
					redirect_section_id = None

				page_queue.put((page_id, page_title, page_text, redirect_article_title, redirect_section_id))

			for _ in range(number_of_workers):
				page_queue.put(None)  # End of data marker

	def parse_page(self, page_queue: mp.Queue, parsed_page_queue: mp.Queue):
		parser = WikitextParser()

		total_number_of_skipped_paragraphs = 0
		number_of_pages_with_skipped_paragraphs = 0
		number_of_paragraphs = 0

		while True:
			page_tuple = page_queue.get()
			if page_tuple is None:
				break

			page_id, page_title, page_text, redirect_article_title, redirect_section_id = page_tuple

			parsed_page = Page(page_id, page_title)

			if redirect_article_title is not None:
				parsed_page.redirect_article_title = redirect_article_title
				parsed_page.redirect_section_id = redirect_section_id
			else:
				try:
					parsed_page.sections, number_of_skipped_paragraphs = parser.parse(page_text)
				except mwp.parser.ParserError:
					print("[Warning] Skipped page '{}' ({:d}) because of parser error.".format(page_title, page_id))
					continue

				if number_of_skipped_paragraphs > 0:
					# print("[Warning] Skipped {:d} paragraph(s) on page '{}' ({:d}) because they contain Wikicode.".format(
					# 	number_of_skipped_paragraphs,
					# 	page_title,
					# 	page_id
					# ))
					total_number_of_skipped_paragraphs += number_of_skipped_paragraphs
					number_of_pages_with_skipped_paragraphs += 1

				number_of_paragraphs += sum(map(lambda x: len(x.paragraphs), parsed_page.sections))

			parsed_page_queue.put(parsed_page)

		print("[Info] Found {:d} paragraphs. Skipped {:d} paragraphs on {:d} pages.".format(
			number_of_paragraphs,
			total_number_of_skipped_paragraphs,
			number_of_pages_with_skipped_paragraphs
		))

		parsed_page_queue.put(None)  # End of data marker

	def extract_paragraphs(self, page_output_queue: mp.Queue, number_of_workers: int = 4, print_progress: bool = True):
		"""
		Extracts paragraphs and links from Wikipedia dump.
		:param page_output_queue: Multiprocessing Queue. Will be filled with Page objects. None is inserted as end marker by each worker.
		:param number_of_workers: Number of threads used for parsing pages.
		:param print_progress: Show progress indicator
		:return: Duration of operation in seconds
		"""

		start_time = datetime.datetime.now()

		progress = mp.Value('d', 0.0)
		is_done = mp.Value('i', 0)

		page_queue = mp.Queue(1000)

		data_reader_process = mp.Process(target=self.read_pages, args=(page_queue, number_of_workers, progress))
		data_reader_process.start()

		worker_processes = []
		for _ in range(number_of_workers):
			worker_process = mp.Process(target=self.parse_page, args=(page_queue, page_output_queue))
			worker_process.start()
			worker_processes.append(worker_process)

		if print_progress:
			progress_process = mp.Process(target=self.print_progress, args=(progress, is_done))
			progress_process.start()
		else:
			progress_process = None

		data_reader_process.join()
		print("Waiting for workers to finish...")

		for worker_process in worker_processes:
			worker_process.join()

		if progress_process is not None:
			is_done.value = 1
			progress_process.join()

		end_time = datetime.datetime.now()
		duration = (end_time - start_time).total_seconds()

		return duration
