import re

from .utils import normalize_page_title


class Template:
	def __init__(self, wikitext):
		self.wikitext = wikitext

		name_match = re.match(r"^{{\s*(.*?)\s*((\|)|(}}))", wikitext, flags=re.DOTALL)
		if name_match is not None:
			self.name = normalize_page_title(name_match.group(1).strip())
		else:
			print("[Warning] Could not parse template name!")
			self.name = None


class Table:
	def __init__(self, wikitext):
		self.wikitext = wikitext


class WikitextPreprocessor:
	"""Simplifies wikitext before parsing. Expands templates, removes tables, and simplifies lists."""

	def __init__(self, template_functions: dict):
		self.template_functions = template_functions

	def tokenize(self, page_text: str, table_mode=False):
		page_text = re.sub(
			r"(<\s*?onlyinclude\s*?>.*?<\s*?/\s*?onlyinclude\s*?>)|(<\s*?includeonly\s*?>.*?<\s*?/\s*?includeonly\s*?>)|(<\s*?ref\s*?>.*?<\s*?/\s*?ref\s*?>)|(<!--((?!-->).)*.?-->)",
			"", page_text, flags=(re.IGNORECASE | re.DOTALL))
		page_text = re.sub(r"(\[\[[^\[\]\n]*?)\n([^\[\]\n]*?\]\])", "\g<1> \g<2>", page_text)

		clean_page_text = ""

		match_iter = re.finditer(
			r"(<\s*?nowiki\s*?>.*?<\s*?/\s*?nowiki\s*?>)|(<\s*?math\s*?>.*?<\s*?/\s*?math\s*?>)|(<\s*?ce\s*?>.*?<\s*?/\s*?ce\s*?>)|(<\s*?chem\s*?>.*?<\s*?/\s*?chem\s*?>)|(<\s*?math\s*?chem\s*?>.*?<\s*?/\s*?math\s*?>)",
			page_text, flags=(re.IGNORECASE | re.DOTALL))
		old_end = 0
		for match in match_iter:
			start = match.start(0)
			end = match.end(0)

			clean_page_text += page_text[old_end:start]
			clean_page_text += " " * (end - start)

			old_end = end

		clean_page_text += page_text[old_end:]

		assert len(clean_page_text) == len(page_text)

		token_ranges = []
		expected_closing_brackets_stack = []

		i = 0
		while i < len(clean_page_text) - 1:
			if not table_mode and clean_page_text[i:i + 3] == "{{{":
				expected_closing_brackets_stack.append(("}}}", i))
				i += 3
			elif not table_mode and clean_page_text[i:i + 2] == "{{":
				expected_closing_brackets_stack.append(("}}", i))
				i += 2
			elif table_mode and clean_page_text[i:i + 2] == "{|":
				expected_closing_brackets_stack.append(("|}", i))
				i += 2
			elif len(expected_closing_brackets_stack) > 0 and expected_closing_brackets_stack[-1][0] == clean_page_text[
																										i:i + 2]:
				closing_brackets, brackets_start_index = expected_closing_brackets_stack.pop()
				if closing_brackets == "}}":
					token_ranges.append((Template, brackets_start_index, i + 2))
				elif closing_brackets == "|}":
					token_ranges.append((Table, brackets_start_index, i + 2))
				i += len(closing_brackets)
			else:
				i += 1

		for missing_closing_tag, brackets_start_index in expected_closing_brackets_stack:
			if missing_closing_tag == "}}":
				token_ranges.append((Template, brackets_start_index, i))
			elif missing_closing_tag == "|}":
				token_ranges.append((Table, brackets_start_index, i))

		tokens = []
		offset = 0

		token_ranges = sorted(token_ranges, key=lambda x: x[1])

		last_range = (-1, -1)
		for token_class, start_index, end_index in token_ranges:
			if start_index >= last_range[0] and end_index <= last_range[1]:
				continue

			assert last_range[1] <= start_index < end_index

			if start_index - offset > 0:
				tokens.append(page_text[offset:start_index])

			token = token_class(page_text[start_index:end_index])
			tokens.append(token)

			offset = end_index

			last_range = (start_index, end_index)

		if offset < len(page_text):
			tokens.append(page_text[offset:])

		return tokens

	def remove_tables(self, tokens: list):
		text = "".join(tokens)
		tokens = self.tokenize(text, table_mode=True)
		return [token for token in tokens if not isinstance(token, Table)]

	def expand_templates(self, tokens: list, unknown_templates_replacement: str = " "):
		output = []

		for token in tokens:
			if isinstance(token, Template):
				template_function = self.template_functions.get(token.name, None)
				if template_function is None:
					if unknown_templates_replacement != "":
						token = unknown_templates_replacement
				elif isinstance(template_function, str):
					token = template_function
				else:
					token = template_function(token)

			output.append(token)

		return output

	def simplify_lists(self, tokens: list):
		text = "".join(tokens)
		text = re.sub(r"^[#*;:]+\s*?(.*)$", r"\n\n\1\n\n", text, flags=re.MULTILINE)  # Replace wikitext lists
		text = re.sub(r"<\s*(/\s*)?((ol)|(ul)|(li)).*?>", r"\n\n", text)
		return [text]

	def remove_style_tags(self, tokens: list):
		output = []

		for token in tokens:
			token = re.sub(r"'{2,3}", " ", token)
			output.append(token)

		return output

	def preprocess_text(self, page_text: str):
		tokens = self.tokenize(page_text)
		tokens = self.expand_templates(tokens)
		tokens = self.remove_tables(tokens)
		tokens = self.simplify_lists(tokens)
		tokens = self.remove_style_tags(tokens)
		preprocessed_text = "".join(tokens)

		return preprocessed_text
