#!/usr/bin/env python3

import urllib.parse
import re
import mwparserfromhell as mwp


def normalize_page_title(title):
	if title is None:
		return None
	
	if isinstance(title, mwp.wikicode.Wikicode):
		title = title.strip_code()
	
	title = title.strip()
	
	if len(title) > 0:
		title = title.replace(" ", "_")
		
		if title[0] != "ß":
			title = title[0].upper() + title[1:]
	
	return title


def normalize_section_title(title):
	if title is None:
		return None
	
	if isinstance(title, mwp.wikicode.Wikicode):
		title = title.strip_code()
	
	title = title.replace(" ", "_")
	title = urllib.parse.quote(title, safe=":")
	title = title.replace("%", ".")
	return title


def group_title(title):
	if title is None:
		return None
	elif len(title) == 0:
		return ""
	
	title = title.replace("_", " ")
	title = title.lower()
	
	group_title_match = re.match(r"^(.*) \(.*?\)$", title)
	if group_title_match is not None:
		group_title = group_title_match.group(1)
	else:
		group_title = title
	
	no_article_group_title_match = re.match(r"^((a)|(an)|(the)) (.*)$", group_title)
	if no_article_group_title_match is not None:
		no_article_group_title = no_article_group_title_match.group(5)
	else:
		no_article_group_title = group_title
	
	return no_article_group_title
