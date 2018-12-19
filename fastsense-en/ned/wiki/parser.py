import re

import mwparserfromhell as mwp

from .templates import TEMPLATE_MAP
from .utils import normalize_page_title, normalize_section_title
from .preprocessor import WikitextPreprocessor


class Link:
	def __init__(self):
		self.range = None
		self.title = None
		self.linked_article_title = None
		self.linked_section_id = None

	def __repr__(self):
		return "<Link range={}; title='{}'; linked_article_title={}; linked_section_id={}>".format(
			str(self.range),
			self.title,
			str(self.linked_article_title),
			str(self.linked_section_id)
		)


class Paragraph:
	def __init__(self):
		self.text = None
		self.links = []

	def __repr__(self):
		return "<Paragraph length={:d}; link_count={:d}>".format(
			len(self.text),
			len(self.links)
		)


class Section:
	def __init__(self, index, title, ids):
		self.index = index
		self.parent_index = None
		self.title = title
		self.ids = ids
		self.paragraphs = []

	def __repr__(self):
		return "<Section index={:d}; parent_index={}; title='{}'; ids={}; paragraph_count={:d}>".format(
			self.index,
			str(self.parent_index),
			self.title,
			self.ids,
			len(self.paragraphs)
		)


class WikitextParser:
	def __init__(self):
		self.preprocessor = WikitextPreprocessor(TEMPLATE_MAP)

		self.ignored_tags = [
			"ref",
			"gallery",
			"score",
			"math",
			"ce",
			"sub",
			"sup",
			"pre",
			"table",
			"imagemap",
			"timeline",
			"source",
			"syntaxhighlight",
			"onlyinclude"
		]
		self.paragraph_tags = ["p", "blockquote"]

	def get_text_and_links(self, nodes, link_offset=0):
		text = ""
		links = []

		for node in nodes:
			if isinstance(node, mwp.nodes.external_link.ExternalLink):
				if node.title is not None:
					text += node.title.strip_code()
				else:
					text += "URL"
			elif isinstance(node, mwp.nodes.html_entity.HTMLEntity):
				text += node.normalize()
			elif isinstance(node, mwp.nodes.tag.Tag):
				if node.tag == "br":
					text += "\n"
				elif node.tag.lower() in self.ignored_tags:
					pass
				else:
					add_linebreaks = node.tag in self.paragraph_tags
					if add_linebreaks:
						text += "\n\n"

					if node.contents is not None:
						tag_text, tag_links = self.get_text_and_links(node.contents.nodes, link_offset=link_offset + len(text))
						text += tag_text
						links += tag_links

					if add_linebreaks:
						text += "\n\n"
			elif isinstance(node, mwp.nodes.text.Text):
				node_text = node.value
				if len(links) > 0 and links[-1].range[1] == link_offset + len(text):
					# Handle link title blending
					match = re.match(r"^[a-zA-Z]+", node_text)
					if match is not None:
						link_part = match.group(0)
						link_range = links[-1].range
						links[-1].range = (link_range[0], link_range[1] + len(link_part))
						links[-1].title += link_part
				text += node_text
			elif isinstance(node, mwp.nodes.wikilink.Wikilink):
				link_destination = str(node.title)
				convert_link_to_text = False

				namespace_match = re.match(r"^(.*?):", link_destination)
				if namespace_match is not None:
					namespace = namespace_match.group(1).capitalize()
					if namespace == "Category" or namespace == "File" or namespace == "Image":
						continue  # Ignore embedded images/audio and category links
					else:
						convert_link_to_text = True

				link_title = node.text.strip_code() if node.text is not None else ""
				if link_title == "":
					link_title = node.title.strip_code()

				stripped_link_title = link_title.lstrip()
				lstrip_size = len(link_title) - len(stripped_link_title)
				text += link_title[:lstrip_size]

				stripped_link_title = stripped_link_title.rstrip()

				link_range_start = link_offset + len(text)
				text += stripped_link_title
				link_range_end = link_offset + len(text)

				rstrip_size = len(link_title) - len(stripped_link_title) - lstrip_size
				text += link_title[len(link_title) - rstrip_size:]

				if link_range_end <= link_range_start:
					convert_link_to_text = True

				if not convert_link_to_text and len(link_title.strip()) > 0:
					components = link_destination.split("#", 1)
					linked_article = normalize_page_title(components[0])
					linked_section = normalize_section_title(components[1]) if len(components) > 1 else None

					link = Link()
					link.title = stripped_link_title
					link.range = (link_range_start, link_range_end)
					link.linked_article_title = linked_article
					link.linked_section_id = linked_section

					links.append(link)

		return text, links

	def get_paragraphs(self, section_nodes: list) -> (list, int):
		"""
		Extracts paragraphs from parsed wikitext nodes.

		Args:
			section_nodes: List of mwp.nodes.Node instances

		Returns:
			List of Paragraph instances and number of skipped paragraphs
		"""

		text, links = self.get_text_and_links(section_nodes)
		split_text = re.split(r"(\s*\n\s*\n\s*)", text)

		paragraphs = []
		skipped_paragraphs_count = 0

		offset = 0

		next_link = links.pop(0) if len(links) > 0 else None
		next_link_range = None

		for i in range(len(split_text)):
			if i % 2 == 0:  # Every second item is whitespace
				split_paragraph_text = re.split(r"(\s{2,}|\n)", split_text[i])

				clean_paragraph_text = ""
				sub_offset = 0
				paragraph_links = []

				for j in range(len(split_paragraph_text)):
					if j % 2 == 0:
						if clean_paragraph_text != "" and len(fragment_text) > 0:
							clean_paragraph_text += " "

						fragment_text = split_paragraph_text[j]
						fragment_start_index_original = offset + sub_offset
						fragment_start_index_clean = len(clean_paragraph_text)
						fragment_length = len(fragment_text)

						clean_paragraph_text += fragment_text

					while next_link is not None and next_link.range[
						0] < fragment_start_index_original + fragment_length:
						if next_link.range[0] >= fragment_start_index_original:
							next_link_range = (
							next_link.range[0] - fragment_start_index_original + fragment_start_index_clean, None)

						if next_link.range[1] <= fragment_start_index_original + fragment_length:
							assert next_link_range is not None

							next_link.range = (next_link_range[0], next_link.range[
								1] - fragment_start_index_original + fragment_start_index_clean)
							next_link.title = re.sub(r"\s{2,}|\n", " ", next_link.title)

							if next_link.title == clean_paragraph_text[next_link.range[0]:next_link.range[1]]:
								paragraph_links.append(next_link)
							else:
								print("[Warning] Skipped link because title doesn't match! ('{}' != '{}')".format(
									next_link.title, clean_paragraph_text[next_link.range[0]:next_link.range[1]]))

							next_link = links.pop(0) if len(links) > 0 else None
							next_link_range = None
						else:
							break

					sub_offset += len(split_paragraph_text[j])

				estimated_token_count = len(clean_paragraph_text.split(" "))
				if len(clean_paragraph_text) > 0:
					if re.search(r"({{)|(}})|({\|)|(\|})|(\[\[)|(\]\])|(\|-)|(^\|)|(align=)|(class=)|(style=)|(cellpadding=)|(cellspacing=)|(border=)|(<div)|(<span)|(<table)|(</)", clean_paragraph_text) is not None:
						skipped_paragraphs_count += 1
					elif estimated_token_count < 5 or len(clean_paragraph_text) < 15 or len(re.findall(r"[A-Za-z]", clean_paragraph_text)) / len(clean_paragraph_text) < 0.5:
						skipped_paragraphs_count += 1
					else:
						paragraph = Paragraph()
						paragraph.text = clean_paragraph_text
						paragraph.links = paragraph_links
						paragraphs.append(paragraph)

			offset += len(split_text[i])

		assert next_link is None

		return paragraphs, skipped_paragraphs_count

	def get_sections(self, wikitext: str) -> list:
		sections = []
		current_section_text = ""

		for line in wikitext.splitlines(keepends=True):
			heading_match = re.match(r"^=+.*?=+\s*$", line)
			if heading_match is not None:
				if len(current_section_text.strip()) > 0:
					sections.append(current_section_text)
				current_section_text = ""

			current_section_text += line

		if len(current_section_text.strip()) > 0:
			sections.append(current_section_text)

		return sections

	def parse(self, wikitext: str) -> (list, int):
		"""
		Parses wikitext and returns parsed sections.

		Args:
			wikitext (str): Wikitext string

		Returns:
			List containing instances of :class:`Section` and number of skipped paragraphs

		Raises:
			WikitextPreprocessor.TokenizationError: Tokenization of templates and tables failed.
			mwp.parser.ParserError: Parsing failed.
		"""

		total_skipped_paragraphs_count = 0

		preprocessed_text = self.preprocessor.preprocess_text(wikitext)
		unparsed_sections = self.get_sections(preprocessed_text)

		sections = []
		section_level_stack = []  # Contains tuple (section index, level) for each section in current parent-child path

		existing_id_strings = set()  # set of id strings
		section_id_string_counts = {}  # dict: key: id, value: count
		existing_anchors = set()  # set of anchor ids

		for section_index in range(len(unparsed_sections)):
			section_nodes = mwp.parse(unparsed_sections[section_index]).nodes

			section_title = None
			section_id_string = None
			other_id_strings = []
			section_level = 2

			# Parse heading
			if len(section_nodes) > 0 and isinstance(section_nodes[0], mwp.nodes.heading.Heading):
				heading_node = section_nodes.pop(0)  # Removes heading from section_nodes
				heading_title = heading_node.title
				section_level = heading_node.level

				for tag in heading_title.filter_tags():
					if tag.has("id"):
						attr = tag.get("id")
						one_id_string = normalize_section_title(attr.value)
						if one_id_string not in existing_anchors:
							existing_anchors.add(one_id_string)
							other_id_strings.append(one_id_string)

				section_title = heading_title.strip_code().strip()
				section_id_string = normalize_section_title(section_title)

				id_str_count = section_id_string_counts.get(section_id_string, 0)
				while True:
					id_str_count += 1
					section_id_string_counts[section_id_string] = id_str_count
					if id_str_count > 1:
						unique_section_id_string = section_id_string + "_" + str(id_str_count)
					else:
						unique_section_id_string = section_id_string

					if unique_section_id_string not in existing_id_strings:
						break

				existing_id_strings.add(unique_section_id_string)
				section_id_string = unique_section_id_string

			section_id_strings = ([section_id_string] if section_id_string is not None else []) + other_id_strings

			# Update stack of parent sections
			while len(section_level_stack) > 0 and section_level_stack[-1][1] >= section_level:
				section_level_stack.pop()  # Reset stack to parent section

			# Create section object
			section = Section(
				index=section_index,
				title=section_title,
				ids=section_id_strings
			)

			if len(section_level_stack) > 0:
				section.parent_index = section_level_stack[-1][0]
			else:
				section.parent_index = None

			# Get paragraphs
			for number_of_nodes_until_next_header in range(len(section_nodes)):
				# section_nodes also contains nodes of subsections. We parse them as own section later.
				if isinstance(section_nodes[number_of_nodes_until_next_header], mwp.nodes.heading.Heading):
					section_nodes = section_nodes[:number_of_nodes_until_next_header]
					break

			section.paragraphs, skipped_paragraphs_count = self.get_paragraphs(section_nodes)

			total_skipped_paragraphs_count += skipped_paragraphs_count

			# Update list of all sections
			sections.append(section)
			section_level_stack.append((section_index, section_level))

		return sections, total_skipped_paragraphs_count
