from typing import Optional, List, Set


class Token:
	"""
	Token class
	"""

	def __init__(
			self,
			start: int,
			end: int,
			value: str,
			pos: Optional[str] = None,
			lemma: Optional[str] = None,
			before: Optional[str] = None,
			after: Optional[str] = None
	):
		"""
		Initializer for token.

		:param start: Start index
		:param end: End index
		:param value: Token text
		:param pos: Part of Speech
		:param lemma: Lemma
		:param before: Text before token that's not part of the previous token. Usually whitespace.
		:param after: Text after token that's not part of the previous token. Usually whitespace.
		"""

		assert value is not None

		self.start = start
		self.end = end
		self.value = value
		self.pos = pos
		self.lemma = lemma
		self.before = before
		self.after = after

	def __repr__(self):
		return "<Token range={:d}:{:d} value='{}' pos='{}' lemma='{}'>".format(
			self.start,
			self.end,
			self.value,
			self.pos,
			self.lemma
		)

	@staticmethod
	def join(tokens: List["Token"], use_lemma: bool = False, keep_before_and_after: bool = False) -> str:
		"""
		Converts list of tokens to string.

		:param tokens: List of tokens
		:param keep_before_and_after: If True, include before and after of first and last token in output string
		:return: Tokens combined as string
		"""
		if len(tokens) == 0:
			return ""

		first_token = tokens[0]
		if keep_before_and_after and first_token.before is not None:
			joined_tokens = first_token.before
		else:
			joined_tokens = ""

		joined_tokens += first_token.value

		for token in tokens[1:]:
			token_value = token.lemma if use_lemma else token.value
			joined_tokens += (token.before if token.before is not None else " ") + token_value

		last_token = tokens[-1]
		if keep_before_and_after and last_token.after is not None:
			joined_tokens += last_token.after

		return joined_tokens
