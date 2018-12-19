#!/usr/bin/env python3

import os.path
import bz2
import xml.sax


class Element(xml.sax.handler.ContentHandler):

	def __init__(self, parent, tag):
		super().__init__()

		self.parent = parent
		self.parser = parent.parser
		self.tag = tag

		self.path = parent.path

		self.parser.setContentHandler(self)

		self.current_tag = None
		self.current_value = None

	def startElement(self, name, attrs):
		self.current_tag = name
		self.current_value = ""

		self.path.append(name)

	def endElement(self, name):
		self.path.pop()

		if name == self.tag:
			self.parser.setContentHandler(self.parent)

			self.current_tag = None
			self.current_value = None

	def characters(self, content):
		if self.current_value is not None and content is not None:
			self.current_value += content


class SiteInfo(Element):
	def __init__(self, parent):
		super().__init__(parent, "siteinfo")


class Page(Element):
	def __init__(self, parent):
		super().__init__(parent, "page")

		self.title = None
		self.ns = None
		self.id = None
		self.redirect = None
		self.restrictions = []
		self.revisions = []
		self.uploads = []

	def __repr__(self):
		return "<Page\ntitle='" + str(self.title) + "';\nns=" + str(self.ns) + "; id=" + str(self.id) + ";\nredirect=" + (
			str("'" + self.redirect + "'") if self.redirect is not None else str(None)
		) + ";\nrestrictions=" + str(self.restrictions) + ";\nrevisions=" + str(self.revisions) + "\nuploads=" + str(self.uploads) + ">"

	def startElement(self, name, attrs):
		super().startElement(name, attrs)

		if name == "redirect":
			self.redirect = attrs["title"]
		elif name == "revision":
			self.revisions += [Revision(parent=self)]
		elif name == "upload":
			self.uploads += [Upload(parent=self)]

	def endElement(self, name):
		super().endElement(name)

		if name == self.tag:
			pass
		elif name == "title":
			self.title = self.current_value
		elif name == "ns":
			self.ns = int(self.current_value)
		elif name == "id":
			self.id = int(self.current_value)
		elif name == "redirect":
			pass
		elif name == "restrictions":
			self.restrictions += [self.current_value]
		else:
			raise Exception("Unexpected XML tag: " + name + " in /" + "/".join(self.path))


class Revision(Element):
	def __init__(self, parent):
		super().__init__(parent, "revision")

		self.id = None
		self.parent_ids = []
		self.timestamp = None
		self.contributor = None
		self.minor = False
		self.comment = None
		self.text = None
		self.sha1 = None
		self.model = None
		self.format = None

	def __repr__(self):
		return "<Revision\nid=" + str(self.id) + "; parent_ids=" + str(self.parent_ids) + "\ntimestamp=" + str(
			self.timestamp) + "\nminor=" + str(self.minor) + "\ncomment='" + str(self.comment) + "'\nmodel='" + str(
			self.model) + "'\nformat='" + str(self.format) + "'\ntext='" + str(self.text) + "'>"

	def startElement(self, name, attrs):
		super().startElement(name, attrs)

		if name == "contributor":
			self.contributor = Contributor(parent=self)
		elif name == "text":
			self.text = Text(parent=self, attrs=attrs)

	def endElement(self, name):
		super().endElement(name)

		if name == self.tag:
			pass
		elif name == "id":
			self.id = int(self.current_value)
		elif name == "parentid":
			self.parent_ids += [int(self.current_value)]
		elif name == "timestamp":
			self.timestamp = self.current_value
		elif name == "minor":
			self.minor = True
		elif name == "comment":
			self.comment = self.current_value
		elif name == "sha1":
			self.sha1 = self.current_value
		elif name == "model":
			self.model = self.current_value
		elif name == "format":
			self.format = self.current_value
		else:
			raise Exception("Unexpected XML tag!", name)


class Contributor(Element):
	def __init__(self, parent):
		super().__init__(parent, "contributor")


class Text(Element):
	def __init__(self, parent, attrs):
		super().__init__(parent, "text")

		self.space = attrs.get("xml:space", "preserve")

		self.deleted = attrs.get("deleted", None) == "deleted"
		assert (attrs.get("deleted", "deleted") == "deleted")

		self.id = attrs.get("id", None)

		bytes_or_none = attrs.get("bytes", None)
		self.bytes = int(bytes_or_none) if bytes_or_none is not None else None

		self.text = ""

	def characters(self, content):
		self.text += content

	def __repr__(self):
		return "space='" + str(self.space) + "'; deleted=" + str(self.deleted) + "; id=" + str(
			self.id) + "; bytes=" + str(self.bytes) + "\n" + self.text


class Upload(Element):
	def __init__(self, parent):
		super().__init__(parent, "upload")


class WikiDumpReader(Element):
	def __init__(self, path):
		self.parser = xml.sax.make_parser()
		self.path = []
		super().__init__(parent=self, tag="mediawiki")

		self.decompressor = bz2.BZ2Decompressor()
		self.file = open(path, "rb")

		self.pages = []

		self.site_info = None
		self.current_page = None

		self.bytes_read = 0
		self.total_bytes = os.path.getsize(path)

	def close(self):
		self.file.close()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()

	def startElement(self, name, attrs):
		super().startElement(name, attrs)

		if name == self.tag:
			pass
		elif name == "siteinfo":
			self.site_info = SiteInfo(parent=self)
		elif name == "page":
			if self.current_page is not None:
				self.pages += [self.current_page]

			self.current_page = Page(parent=self)
		else:
			raise Exception("Unexpected XML tag!", name)

	def endElement(self, name):
		super().endElement(name)

		if name == self.tag:
			if self.current_page is not None:
				self.pages += [self.current_page]
		else:
			raise Exception("Unexpected XML tag!", name)

	def parse_next_chunk(self, length=900000):
		compressed_data = self.file.read(length)
		if len(compressed_data) > 0:
			uncompressed_data = self.decompressor.decompress(compressed_data)
			if uncompressed_data is not None and len(uncompressed_data) > 0:
				self.parser.feed(uncompressed_data)

			self.bytes_read += len(compressed_data)

	def next_page(self):
		while len(self.pages) == 0 and not self.decompressor.eof:
			self.parse_next_chunk()

		if len(self.pages) == 0:
			return None

		next_page = self.pages[0]
		self.pages = self.pages[1:]

		return next_page
