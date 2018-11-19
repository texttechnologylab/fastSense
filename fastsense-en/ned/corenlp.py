import multiprocessing as mp
import queue
from typing import Dict, List, Tuple, Optional
from .token import Token


def _corenlp_server(classpath: str, properties: Dict[str, str], input_queue: mp.Queue, output_queue: mp.Queue):
	# Imports are inside function because pyjnius is ugly

	import jnius_config
	jnius_config.add_options('-Xmx2G')
	jnius_config.add_options('-Xss1280k')  # Needed because bug in linux kernel
	jnius_config.set_classpath(classpath)

	import jnius
	import ftfy.bad_codecs  # provides "utf-8-variants"

	StanfordCoreNLP = jnius.autoclass('edu.stanford.nlp.pipeline.StanfordCoreNLP')
	Annotation = jnius.autoclass('edu.stanford.nlp.pipeline.Annotation')
	PropertiesClass = jnius.autoclass('java.util.Properties')
	SentencesAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$SentencesAnnotation')
	TokensAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$TokensAnnotation')
	OriginalTextAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$OriginalTextAnnotation')
	BeforeAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$BeforeAnnotation')
	AfterAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$AfterAnnotation')
	LemmaAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$LemmaAnnotation')
	PartOfSpeechAnnotation = jnius.autoclass('edu.stanford.nlp.ling.CoreAnnotations$PartOfSpeechAnnotation')

	String = jnius.autoclass('java.lang.String')
	StandardCharsets = jnius.autoclass('java.nio.charset.StandardCharsets')

	props = PropertiesClass()
	for key, value in properties.items():
		key_java_string = String(key.encode("utf-8"), StandardCharsets.UTF_8)
		value_java_string = String(value.encode("utf-8"), StandardCharsets.UTF_8)
		props.setProperty(key_java_string, value_java_string)

	annotators = properties.get("annotators", "").split(",")
	annotators = list(map(str.lower, map(str.strip, annotators)))
	has_lemma = "lemma" in annotators
	has_pos = "pos" in annotators

	pipeline = StanfordCoreNLP(props)

	def get_annotation(token, annotation):
		try:
			return token.get(annotation)
		except UnicodeDecodeError as e:
			# Pyjnius returns strings in Java-internal modified utf-8 encoding, which isn't 100% compatible with utf-8.
			return e.object.decode("utf-8-variants")

	while True:
		job = input_queue.get()
		if job is None:
			break

		job_id, text, offset = job

		if text == "":
			output_queue.put((job_id, []))
			continue

		utf8_bytes = text.encode("utf-8")
		java_string = String(utf8_bytes, StandardCharsets.UTF_8)

		document = Annotation(java_string)
		pipeline.annotate(document)
		sentence_iter = document.get(SentencesAnnotation).iterator()

		reconstructed_text = ""
		after = ""

		sentences = []
		while sentence_iter.hasNext():
			token_iter = sentence_iter.next().get(TokensAnnotation).iterator()

			sentence_tokens = []
			while token_iter.hasNext():
				token = token_iter.next()

				original_text = get_annotation(token, OriginalTextAnnotation)
				lemma = get_annotation(token, LemmaAnnotation) if has_lemma else None
				pos = get_annotation(token, PartOfSpeechAnnotation) if has_pos else None
				before = get_annotation(token, BeforeAnnotation)
				after = get_annotation(token, AfterAnnotation)
				# start and end index are not used because they may be different from Python's string indexes.

				reconstructed_text += before
				begin = len(reconstructed_text) + offset
				reconstructed_text += original_text
				end = len(reconstructed_text) + offset

				token_object = Token(
					start=begin,
					end=end,
					value=original_text,
					pos=pos,
					lemma=lemma,
					before=before,
					after=after
				)
				sentence_tokens.append(token_object)

			sentences.append(sentence_tokens)

		reconstructed_text += after
		if reconstructed_text != text:
			output_queue.put((job_id, None))
		else:
			output_queue.put((job_id, sentences))


class CoreNlpBridge:
	"""
	Bridge between CoreNLP (Java) and Python.
	"""

	class TokenizationError(Exception):
		pass

	def __init__(self, classpath: str, properties: Optional[Dict[str, str]] = None, process_count: Optional[int] = None):
		"""
		Initializes CoreNLP bridge.

		:param classpath: Path to CoreNLP Java classes. e.g. "./corenlp/*"
		:param properties: Dict containing properties for CoreNLP. e.g. {"annotators": "tokenize,ssplit,pos,lemma"}
		:param process_count: Number of processes used for tokenization. Uses CPU count if None.
		"""
		assert classpath is not None
		assert process_count is None or process_count > 0

		if process_count is None:
			process_count = mp.cpu_count()

		if properties is None:
			properties = {
				"annotators": "tokenize,ssplit,pos,lemma",
				"tokenize.options": "untokenizable=noneKeep,invertible=true,ptb3Escaping=false",
				"tokenize.language": "en"
			}

		self.in_queue = mp.Queue(process_count)
		self.out_queue = mp.Queue(process_count)

		corenlp_processes = []
		args = (classpath, properties, self.in_queue, self.out_queue)

		for i in range(process_count):
			corenlp_process = mp.Process(target=_corenlp_server, args=args)
			corenlp_process.start()
			corenlp_processes.append(corenlp_process)

		self.corenlp_processes = corenlp_processes

	def close(self):
		"""
		Closes bridge. Call this method after you're done or use a `with` statement.
		"""
		in_queue = self.in_queue
		corenlp_processes = self.corenlp_processes

		self.corenlp_processes = None
		self.in_queue = None
		self.out_queue = None

		for _ in range(len(corenlp_processes)):
			in_queue.put(None)

		for corenlp_process in corenlp_processes:
			corenlp_process.join()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()
		return False

	def tokenize(self, paragraphs: List[Tuple[int, str]]) -> List[List[List[Token]]]:
		"""
		Tokenizes text and splits it into sentences.

		:param paragraphs: List of paragraphs. Each paragraph is a tuple containing offset and text.
		:return: List of paragraphs. Each paragraph is a list of sentences. Each sentence is a list of tokens of type Token.
		"""

		assert self.corenlp_processes is not None

		remaining_jobs = []
		for paragraph_index in range(len(paragraphs)):
			offset, text = paragraphs[paragraph_index]
			remaining_jobs.append((paragraph_index, text, offset))

		output = []
		while True:
			while len(remaining_jobs) > 0:
				next_job = remaining_jobs.pop(0)
				try:
					self.in_queue.put(next_job, block=False)
				except queue.Full:
					remaining_jobs.insert(0, next_job)
					break

			paragraph_index, sentences = self.out_queue.get()
			if sentences is None:
				raise CoreNlpBridge.TokenizationError()

			output.append((paragraph_index, sentences))

			if len(output) == len(paragraphs):
				break

		return list(map(lambda x: x[1], sorted(output, key=lambda x: x[0])))
