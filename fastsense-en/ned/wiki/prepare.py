import multiprocessing as mp
import sqlite3
import gzip
import re
import os
from typing import List

from .extractor import WikiExtractor
from .utils import normalize_page_title, group_title
from ..corenlp import CoreNlpBridge


class WikiConverter:

	@staticmethod
	def run(dump_path: str, page_table_path: str, categorylinks_table_path: str, db_path: str, output_path: str, output_file_count: int, corenlp_classpath: str, test_set_sizes: List[float], print_progress: bool = True):
		os.makedirs(os.path.dirname(db_path), exist_ok=True)
		os.makedirs(output_path, exist_ok=True)

		number_of_workers = 6

		page_queue = mp.Queue(1000)
		sql_queue = mp.Queue(10000)
		paragraph_queue = mp.Queue(1000)
		links_queue = mp.Queue(1000)
		count_queue = mp.Queue(10000)

		sql_queue.put("DROP TABLE IF EXISTS 'articles'")
		sql_queue.put("""
		CREATE TABLE "articles" (
			"id" INTEGER PRIMARY KEY,
			"title" TEXT,
			"group_title" TEXT,
			"redirect_article_title" TEXT,
			"redirect_section_id" TEXT,
			"is_disambig" INTEGER
		)
		""")
		sql_queue.put("DROP TABLE IF EXISTS 'temp_sections'")
		sql_queue.put("""
		CREATE TABLE "temp_sections" (
			"article_id" INTEGER,
			"section_index" INTEGER,
			"parent_index" INTEGER,
			"title" TEXT
		)
		""")
		sql_queue.put("DROP TABLE IF EXISTS 'section_ids'")
		sql_queue.put("""
		CREATE TABLE "section_ids" (
			"article_id" INTEGER,
			"section_index" INTEGER,
			"id_string" TEXT
		)
		""")

		tokens_queues = []
		data_processes = []

		for i in range(output_file_count):
			tokens_queue = mp.Queue(1000)
			tokens_queues.append(tokens_queue)

			data_process = mp.Process(target=WikiConverter.file_task, args=(os.path.join(output_path, "tokens_" + str(i) + ".gz"), tokens_queue))
			data_process.start()
			data_processes.append(data_process)

		processes = []
		for _ in range(number_of_workers):
			worker_process = mp.Process(target=WikiConverter.worker_task, args=(page_queue, sql_queue, paragraph_queue))
			worker_process.start()
			processes.append(worker_process)

			tokenize_process = mp.Process(target=WikiConverter.tokenize_task, args=(paragraph_queue, tokens_queues, links_queue, count_queue, corenlp_classpath))
			tokenize_process.start()
			processes.append(tokenize_process)

		sql_process = mp.Process(target=WikiConverter.db_task, args=(db_path, sql_queue))
		sql_process.start()

		links_path = os.path.join(output_path, "links.gz")
		links_process = mp.Process(target=WikiConverter.file_task, args=(links_path, links_queue))
		links_process.start()

		count_process = mp.Process(target=WikiConverter.count_task, args=(sql_queue, count_queue))
		count_process.start()

		extractor = WikiExtractor(dump_path=dump_path)
		extractor.extract_paragraphs(
			page_output_queue=page_queue,
			number_of_workers=number_of_workers,
			print_progress=print_progress
		)

		for process in processes:
			process.join()

		for tokens_queue in tokens_queues:
			tokens_queue.put(None)

		links_queue.put(None)
		count_queue.put(None)

		for data_process in data_processes:
			data_process.join()

		links_process.join()
		count_process.join()

		sql_queue.put(None)
		sql_process.join()

		WikiConverter.update_total_paragraph_counts(db_path)
		WikiConverter.update_disambig_page_flags(db_path, page_table_path, categorylinks_table_path)
		WikiConverter.count_links(db_path, links_path)
		WikiConverter.find_senses(db_path)
		WikiConverter.divide_data(db_path, test_set_sizes=test_set_sizes)

	@staticmethod
	def update_total_paragraph_counts(db_path: str):
		conn = sqlite3.connect(db_path)
		c = conn.cursor()
		c2 = conn.cursor()

		print("Updating total paragraph counts")

		conn.executescript("""
			DROP TABLE IF EXISTS 'sections';
			CREATE TABLE "sections" (
				"article_id" INTEGER,
				"section_index" INTEGER,
				"parent_index" INTEGER,
				"title" TEXT,
				"paragraph_count" INTEGER,
				"total_paragraph_count" INTEGER
			);
		""")

		insert_buffer = []

		article_id = None
		article_sections = []

		sql = """
			select
				S.article_id,
				S.section_index,
				S.parent_index,
				S.title,
				ifnull(C.paragraph_count, 0)
			from
				temp_sections S
			left join
				section_paragraph_counts C
				on S.article_id = C.article_id
				and S.section_index = C.section_index
			order by
				S.article_id asc,
				S.section_index asc
		"""

		sql2 = "insert into sections values (?,?,?,?,?,?)"

		for a_id, s_idx, parent_idx, title, count in c.execute(sql):
			if a_id != article_id:
				if article_id is not None:
					insert_buffer += article_sections

					if len(insert_buffer) >= 500:
						c2.executemany(sql2, insert_buffer)
						insert_buffer = []

				article_id = a_id
				article_sections = []

			assert len(article_sections) == s_idx
			article_sections.append((a_id, s_idx, parent_idx, title, count, 0))  # article id, section index, parent index, section title, paragraph count, paragraph count including subsections

			i = s_idx
			while i is not None:
				other_a_id, other_s_idx, other_parent_idx, other_title, other_count, other_total_count = article_sections[i]
				other_total_count += count
				article_sections[i] = (other_a_id, other_s_idx, other_parent_idx, other_title, other_count, other_total_count)
				i = other_parent_idx

		if article_id is not None:
			insert_buffer += article_sections

		if len(insert_buffer) >= 1:
			c2.executemany(sql2, insert_buffer)

		conn.executescript("""
			drop table temp_sections;
			drop table section_paragraph_counts;
		""")

		conn.commit()
		conn.close()

	@staticmethod
	def update_disambig_page_flags(db_path: str, page_table_path: str, categorylinks_table_path: str):
		conn = sqlite3.connect(db_path)
		conn.executescript("""
			DROP TABLE IF EXISTS "categories";
			CREATE TABLE "categories" (
			"id" INTEGER PRIMARY KEY,
			"title" TEXT
			);

			DROP TABLE IF EXISTS "subcategories";
			CREATE TABLE "subcategories" (
			"category_id" INTEGER,
			"subcategory_id" INTEGER
			);

			DROP TABLE IF EXISTS "article_categories";
			CREATE TABLE "article_categories" (
			"article_id" INTEGER,
			"category_id" INTEGER
			);
		""")

		c = conn.cursor()

		# Import categories
		with gzip.open(page_table_path, "rt", encoding="utf8", errors="replace") as f:
			line_prefix = "INSERT INTO `page` VALUES "

			i = 0
			for line in f:
				if line.startswith(line_prefix):
					line = line[len(line_prefix):]
					values = re.findall(r"\(([0-9]+),(-?[0-9]+),'(.*?)(?<!(?<!(?<!\\)\\)\\)',.*?\)[,;]", line)
					for page_id, page_ns, page_title in values:
						page_ns = int(page_ns)
						if page_ns != 14:  # Namespace 14 is for categories
							continue

						page_id = int(page_id)
						page_title = page_title.replace("\\'", "'")
						params = (page_id, page_title)

						c.execute("INSERT INTO categories (id, title) VALUES (?,?)", params)

				i += 1
				print("Importing categories: Line {:d}...".format(i), end="\r")

		conn.executescript("""
			CREATE UNIQUE INDEX "category_titles" ON "categories" ("title");
		""")
		conn.commit()

		print("Importing categories: Line {:d}... Done!".format(i))

		# Import category links
		with gzip.open(categorylinks_table_path, "rt", encoding="utf8", errors="replace") as f:
			line_prefix = "INSERT INTO `categorylinks` VALUES "

			i = 0
			for line in f:
				if not line.startswith(line_prefix):
					continue

				line = line[len(line_prefix):]
				values = re.findall(
					r"\(([0-9]+),'(.*?)(?<!(?<!(?<!\\)\\)\\)','.*?(?<!(?<!(?<!\\)\\)\\)','.*?(?<!(?<!(?<!\\)\\)\\)','.*?(?<!(?<!(?<!\\)\\)\\)','.*?(?<!(?<!(?<!\\)\\)\\)','(.*?)(?<!(?<!(?<!\\)\\)\\)'\)[,;]",
					line,
					flags=re.DOTALL
				)

				for from_id, target_title, link_type in values:
					if link_type != "page" and link_type != "subcat":
						continue

					from_id = int(from_id)
					target_title = target_title.replace("\\'", "'")

					c.execute("SELECT id FROM categories WHERE title = ?", (target_title,))
					target_id_row = c.fetchone()
					if target_id_row is None:
						continue

					target_id = target_id_row[0]

					if link_type == "page":
						c.execute("SELECT id FROM articles WHERE id = ?", (from_id,))
						if c.fetchone() is None:
							continue

						params = (from_id, target_id)
						c.execute("INSERT INTO article_categories (article_id, category_id) VALUES (?,?)", params)
					elif link_type == "subcat":
						params = (target_id, from_id)
						c.execute("INSERT INTO subcategories (category_id, subcategory_id) VALUES (?,?)", params)

				i += 1
				print("Importing category links: Line {:d}...".format(i), end="\r")

		conn.executescript("""
			CREATE INDEX "categories_category_index" ON "article_categories" ("category_id");
			CREATE INDEX "subcategories_index" ON "subcategories" ("category_id");
		""")
		conn.commit()

		print("Importing category links: Line {:d}... Done!".format(i))

		# Update disambig page flag in articles table
		print("Updating disambiguation page flags in articles table...")

		c.execute("UPDATE articles SET is_disambig = 0")

		c.execute("SELECT id FROM categories WHERE title = 'Disambiguation_pages'")
		disambig_page_ids = set()
		category_ids = {c.fetchone()[0]}
		while len(category_ids) > 0:
			category_id = category_ids.pop()
			for row in c.execute("SELECT subcategory_id FROM subcategories WHERE category_id = ?", (category_id,)):
				category_ids.add(row[0])

			for row in c.execute("SELECT article_id FROM article_categories WHERE category_id = ?", (category_id,)):
				disambig_page_ids.add(row[0])

		for disambig_page_id in disambig_page_ids:
			c.execute("UPDATE articles SET is_disambig = 1 WHERE id = ?", (disambig_page_id,))

		conn.commit()
		conn.close()

	@staticmethod
	def count_links(db_path: str, links_path: str):
		conn = sqlite3.connect(db_path)

		print("Creating links table...")
		conn.executescript("""
			DROP TABLE IF EXISTS "links";
			CREATE TABLE "links" (
			"article_id" INTEGER,
			"section_index" INTEGER,
			"paragraph_index" INTEGER,
			"sentence_index" INTEGER,
			"start_index" INTEGER,
			"end_index" INTEGER,
			"linked_article_id" INTEGER,
			"linked_section_index" INTEGER,
			"link_title" TEXT,
			"group_title" TEXT,
			"is_on_disambig_page" INTEGER
			);
		""")

		c = conn.cursor()

		print("Loading metadata for articles...")
		articles = {}  # title -> (id, is_disambig)
		disambig_articles = set()
		for row in c.execute("SELECT id, title, is_disambig FROM articles WHERE redirect_article_title is NULL"):
			article_id, title, is_disambig = row
			articles[title] = article_id
			if is_disambig:
				disambig_articles.add(article_id)

		redirects = {}  # title -> (article title, section id string)
		for row in c.execute("SELECT title, redirect_article_title, redirect_section_id FROM articles WHERE redirect_article_title is not NULL"):
			title, redirect_article_title, redirect_section_id = row
			redirects[title] = (redirect_article_title, redirect_section_id)

		sections = {}  # id string -> section index
		for row in c.execute("SELECT article_id, id_string, section_index FROM section_ids"):
			article_id, id_string, section_index = row
			sections[(article_id, id_string)] = section_index

		print("Resolving multi-redirects...")
		for _ in range(3):
			for redirect_title, destination in redirects.items():
				dest_article_title, dest_section_id = destination

				if dest_article_title in redirects:
					print("Found multi-redirect:", redirect_title, destination)
					new_dest = redirects[dest_article_title]
					redirects[redirect_title] = (
					new_dest[0], new_dest[1] if dest_section_id is None else dest_section_id)

		print("Resolving links...")

		resolved_links = []
		last_disambig_page_article_id = None

		with gzip.open(links_path, "rt", encoding="utf8") as f:
			for line in f:
				if len(resolved_links) >= 200:
					c.executemany("INSERT INTO links VALUES (?,?,?,?,?,?,?,?,?,?,?)", resolved_links)
					resolved_links = []

				line = line[:-1]  # remove \n
				line = line.split("\t")

				article_id, section_index, paragraph_index, sentence_index, start_index, end_index = list(map(int, line[0:6]))
				linked_article, linked_section, link_title = line[6:]

				if article_id == last_disambig_page_article_id:
					is_on_disambig_page = True
				else:
					is_on_disambig_page = article_id in disambig_articles
					if is_on_disambig_page:
						last_disambig_page_article_id = article_id

				if linked_article in redirects:
					linked_article, redirect_linked_section = redirects[linked_article]
					if linked_section == "":
						linked_section = redirect_linked_section

				if linked_article not in articles:
					continue  # e.g. interwiki links

				linked_article_id = articles[linked_article]

				if linked_section is not None and linked_section != "":
					section_key = (linked_article_id, linked_section)
					if section_key not in sections:
						continue

					linked_section_index = sections[section_key]
					if linked_section_index is None:
						linked_section_index = -1
				else:
					linked_section_index = -1

				resolved_link = (
					article_id,
					section_index,
					paragraph_index,
					sentence_index,
					start_index,
					end_index,
					linked_article_id,
					linked_section_index,
					link_title,
					group_title(link_title),
					is_on_disambig_page
				)
				resolved_links.append(resolved_link)

		if len(resolved_links) >= 1:
			c.executemany("INSERT INTO links VALUES (?,?,?,?,?,?,?,?,?,?,?)", resolved_links)

		print("Committing db changes...")
		conn.commit()
		conn.close()

	@staticmethod
	def find_senses(db_path: str):
		conn = sqlite3.connect(db_path)

		print("Grouping links...")
		conn.executescript("""
			-- Create temp table for see also sections
			DROP TABLE IF EXISTS "temp_see_also_sections";
			CREATE TABLE "temp_see_also_sections" (
				"article_id" INTEGER,
				"section_index" INTEGER
			);
			
			-- Find section indexes for 'see also' sections and child sections on disambiguation pages
			with
			root_sections as (select S.article_id, S.section_index, S.parent_index from sections S, articles A where S.article_id = A.id and A.is_disambig = 1 and lower(S.title) = 'see also'),
			child_sections_l1 as (select A.article_id, A.section_index, A.parent_index from sections A, root_sections B where A.article_id = B.article_id and A.parent_index = B.section_index),
			child_sections_l2 as (select A.article_id, A.section_index, A.parent_index from sections A, child_sections_l1 B where A.article_id = B.article_id and A.parent_index = B.section_index),
			child_sections_l3 as (select A.article_id, A.section_index, A.parent_index from sections A, child_sections_l2 B where A.article_id = B.article_id and A.parent_index = B.section_index)
			insert into temp_see_also_sections
			select article_id, section_index from root_sections
			union select article_id, section_index from child_sections_l1
			union select article_id, section_index from child_sections_l2
			union select article_id, section_index from child_sections_l3;
			
			-- Create temp table for links on disambiguation pages
			DROP TABLE IF EXISTS "temp_disambig_links";
			CREATE TABLE "temp_disambig_links" (
				"article_id" INTEGER,
				"group_title" TEXT,
				"linked_article_id" INTEGER,
				"linked_section_index" INTEGER,
				"link_title" TEXT,
				"ignored" INTEGER
			);
			
			-- Find links on disambiguation pages
			insert into temp_disambig_links
			select
				L.article_id,
				A.group_title,
				L.linked_article_id,
				L.linked_section_index,
				L.link_title,
				case when S.section_index is not null then 1 else not instr(lower(L.link_title), A.group_title) end
			from links L, articles A
			left join temp_see_also_sections S
			on L.article_id = S.article_id and L.section_index = S.section_index
			where A.is_disambig = 1 and L.article_id = A.id;
			
			-- drop temp see also sections table
			drop table temp_see_also_sections;
			
			-- Create temp table for links
			DROP TABLE IF EXISTS "temp_links";
			CREATE TABLE "temp_links" (
				"article_id" INTEGER,
				"section_index" INTEGER,
				"paragraph_index" INTEGER,
				"sentence_index" INTEGER,
				"linked_article_id" INTEGER,
				"linked_section_index" INTEGER,
				"group_title" TEXT
			);
			
			insert into temp_links
			select distinct
				article_id,
				section_index,
				paragraph_index,
				sentence_index,
				linked_article_id,
				linked_section_index,
				group_title
			from
				links
			where
				is_on_disambig_page = 0;
			
			-- create temp table for grouped links
			DROP TABLE IF EXISTS "temp_grouped_links";
			CREATE TABLE "temp_grouped_links" (
				"group_title" TEXT,
				"article_id" INTEGER,
				"section_index" INTEGER,
				"is_on_disambig_page" INTEGER,  -- 0 = no, 1 = yes, 2 = yes, but ignored
				"article_paragraph_count" INTEGER,
				"linked_count_matching_title" INTEGER,
				"linked_count_other_title" INTEGER
			);
			
			-- Add link counts
			with total_counts as (
				select
					linked_article_id,
					linked_section_index,
					count(*) as count
				from
					(
						select
							*
						from
							temp_links
						group by
							article_id,
							section_index,
							paragraph_index,
							sentence_index,
							linked_article_id,
							linked_section_index
					)
				group by
					linked_article_id,
					linked_section_index
			),
			matching_counts as (
				select
					group_title,
					linked_article_id,
					linked_section_index,
					count(*) as count
				from
					temp_links
				group by
					group_title,
					linked_article_id,
					linked_section_index
			)
			insert into temp_grouped_links
			select
				M.group_title,
				M.linked_article_id,
				M.linked_section_index,
				0,
				0,
				M.count,
				T.count - M.count
			from
				matching_counts M
			left join total_counts T on
				M.linked_article_id = T.linked_article_id
				and M.linked_section_index = T.linked_section_index;
			
			-- Drop temp table for links
			drop table temp_links;
			
			-- insert links from disambig pages
			insert into temp_grouped_links
			select distinct
				group_title,
				linked_article_id,
				linked_section_index,
				1,
				0,
				0,
				0
			from
				temp_disambig_links
			where
				ignored = 0;
			
			-- add article counts
			with article_paragraph_counts as (
				select
					article_id,
					sum(paragraph_count) as count
				from
					sections
				group by
					article_id
			),
			temp_article_groups as (
				select distinct
					group_title,
					article_id,
					section_index
				from
					temp_grouped_links
				where
					section_index = -1
			)
			insert into temp_grouped_links
			select
				A.group_title,
				A.article_id,
				A.section_index,
				0,
				C.count,
				0,
				0
			from
				temp_article_groups A,
				article_paragraph_counts C
			where
				A.article_id = C.article_id;
			
			-- add section counts
			with temp_section_groups as (
				select distinct
					group_title,
					article_id,
					section_index
				from
					temp_grouped_links
				where
					section_index >= 0
			)
			insert into temp_grouped_links
			select
				S.group_title,
				S.article_id,
				S.section_index,
				0,
				C.total_paragraph_count,
				0,
				0
			from
				temp_section_groups S,
				sections C
			where
				S.article_id = C.article_id
				and S.section_index = C.section_index;
			
			-- create table for grouped links and insert merged rows from temp table
			DROP TABLE IF EXISTS "grouped_links";
			CREATE TABLE "grouped_links" (
				"group_title" TEXT,
				"article_id" INTEGER,
				"section_index" INTEGER,
				"is_on_disambig_page" INTEGER,
				"article_paragraph_count" INTEGER,
				"linked_count_matching_title" INTEGER,
				"linked_count_other_title" INTEGER
			);
			
			insert into grouped_links
			select
				group_title,
				article_id,
				section_index,
				max(is_on_disambig_page),
				max(article_paragraph_count),
				max(linked_count_matching_title),
				max(linked_count_other_title)
			from
				temp_grouped_links
			group by
				group_title,
				article_id,
				section_index;
			
			-- drop temp tables
			drop table temp_disambig_links;
			drop table temp_grouped_links;
		""")

		print("Looking for senses...")
		conn.executescript("""
			DROP TABLE IF EXISTS "raw_senses";
			CREATE TABLE "raw_senses" (
			"group_title" TEXT,
			"article_id" INTEGER,
			"section_index" INTEGER,
			"linked_count_matching_title" INTEGER,
			"linked_count_other_title" INTEGER,
			"article_paragraph_count" INTEGER
			);
			
			with disambig_group_titles as (
				select distinct group_title
				from articles
				where is_disambig = 1
			)
			insert into raw_senses
			select
				group_title,
				article_id,
				section_index,
				linked_count_matching_title,
				linked_count_other_title,
				article_paragraph_count
			from grouped_links
			where
				group_title in disambig_group_titles
				and article_paragraph_count + linked_count_matching_title + linked_count_other_title >= 14 -- at least 14 examples
				and (is_on_disambig_page = 1 or linked_count_matching_title >= 5);  -- not ignored on disambig. page or linked at least 5 times using group title
			
			create unique index if not exists raw_senses_index on raw_senses (group_title, article_id, section_index);
		""")

		print("Building list of groups and senses...")
		conn.executescript("""
			DROP TABLE IF EXISTS "sense_groups";
			CREATE TABLE "sense_groups" (
				"id" INTEGER PRIMARY KEY AUTOINCREMENT,
				"group_title" TEXT
			);
			
			insert into sense_groups (group_title)
			select group_title
			from raw_senses
			group by group_title
			order by sum(article_paragraph_count + linked_count_matching_title + linked_count_other_title) desc;
			
			update sense_groups set id = id - 1;  -- offset id by -1 to make it start at 0
			
			DROP TABLE IF EXISTS "senses";
			CREATE TABLE "senses" (
				"id" INTEGER PRIMARY KEY AUTOINCREMENT,
				"article_id" INTEGER,
				"section_index" INTEGER
			);
			
			insert into senses (article_id, section_index)
			select article_id, section_index
			from raw_senses
			group by article_id, section_index
			order by sum(article_paragraph_count + linked_count_matching_title + linked_count_other_title) desc;
			
			update senses set id = id - 1;  -- offset id by -1 to make it start at 0
			
			DROP TABLE IF EXISTS "sense_group_senses";
			CREATE TABLE "sense_group_senses" (
				"id" INTEGER PRIMARY KEY,
				"sense_group" INTEGER,
				"sense" INTEGER
			);
			
			insert into sense_group_senses (sense_group, sense)
			select
				G.id,
				S.id
			from raw_senses R, sense_groups G, senses S
			where R.group_title = G.group_title and R.article_id = S.article_id and R.section_index = S.section_index
			order by article_paragraph_count + linked_count_matching_title + linked_count_other_title desc;
			
			update sense_group_senses set id = id - 1;  -- offset id by -1 to make it start at 0
		""")

		print("Looking for alternative group titles using redirects to disambig pages...")
		conn.executescript("""
			-- Find alternative group titles using redirects
			DROP TABLE IF EXISTS "alternative_group_titles";
			CREATE TABLE "alternative_group_titles" (
				"group_title" TEXT,
				"alternative_group_title" TEXT
			);
			
			insert into alternative_group_titles
			select
				A.group_title,
				R.group_title
			from
				articles R,
				articles A
			where
				R.redirect_article_title is not null
				and R.redirect_article_title = A.title
				and A.is_disambig = 1
				and R.group_title != A.group_title
			group by R.group_title;
			
			-- Delete alternative titles that are used for multiple groups or match actual groups.
			delete from alternative_group_titles
			where alternative_group_title in (
				select alternative_group_title
				from alternative_group_titles
				group by alternative_group_title having count(*) > 1
			)
			or alternative_group_title in (
				select distinct group_title from sense_groups
			);
		""")

		print("Committing changes...")

		conn.commit()
		conn.close()

	@staticmethod
	def divide_data(db_path: str, test_set_sizes: List[float]):
		conn = sqlite3.connect(db_path)
		c = conn.cursor()

		print("Searching for examples...")
		conn.executescript("""
			DROP TABLE IF EXISTS "data";
			CREATE TABLE "data" (
				"article_id" INTEGER,
				"section_index" INTEGER,
				"paragraph_index" INTEGER,
				"sentence_index" INTEGER,
				"sense_group_sense_id" INTEGER,
				"dataset" INTEGER
			);
			
			create unique index if not exists data_index on data (sense_group_sense_id, article_id, section_index, paragraph_index, sentence_index);
			
			-- Link paragraphs
			insert or ignore into data
			select distinct
				L.article_id,
				L.section_index,
				L.paragraph_index,
				L.sentence_index,
				SGS.id,
				null
			from raw_senses R
			inner join links L on
				L.linked_article_id = R.article_id
				and L.linked_section_index = R.section_index
			left join sense_groups G on G.group_title = R.group_title
			left join senses S on S.article_id = R.article_id and S.section_index = R.section_index
			left join sense_group_senses SGS on SGS.sense_group = G.id and SGS.sense = S.id
			where
				L.is_on_disambig_page = 0
				and SGS.id is not null;
		""")

		# Load sections into RAM for fast access
		sections = {}  # article_id -> [(subsection_index, subsection_parent, paragraph_count)]

		sql = """
			select
				article_id,
				section_index,
				ifnull(parent_index, -1),
				paragraph_count
			from sections
			order by article_id, section_index
		"""

		current_article_id = None
		current_article_sections = []
		for article_id, subsection_index, subsection_parent, paragraph_count in c.execute(sql):
			if article_id != current_article_id:
				if len(current_article_sections) > 0:
					sections[current_article_id] = current_article_sections
					current_article_sections = []
				current_article_id = article_id

			current_article_sections.append((subsection_index, subsection_parent, paragraph_count))

		if len(current_article_sections) > 0:
			sections[current_article_id] = current_article_sections
			del current_article_sections

		sql = """
			select
				R.article_id,
				R.section_index,
				SGS.id
			from raw_senses R
			left join sense_groups G on G.group_title = R.group_title
			left join senses S on S.article_id = R.article_id and S.section_index = R.section_index
			left join sense_group_senses SGS on SGS.sense_group = G.id and SGS.sense = S.id
		"""

		total_p_count = 0

		cached_article_id = None
		cached_article_sections = []

		insert_buffer = []

		c2 = conn.cursor()
		for article_id, section_index, sgs_id in c2.execute(sql):
			if cached_article_id != article_id:
				cached_article_id = article_id
				cached_article_sections = sections.get(article_id, [])

			parent_sections = {section_index}
			for subsection_index, subsection_parent, paragraph_count in cached_article_sections:
				if subsection_parent in parent_sections:
					parent_sections.add(subsection_index)
				elif subsection_index not in parent_sections:
					continue

				for paragraph_index in range(paragraph_count):
					insert_buffer.append(
						(
							article_id,
							subsection_index,
							paragraph_index,
							None,
							sgs_id,
							None
						)
					)

			if len(insert_buffer) >= 500:
				c.executemany("insert or ignore into data values (?,?,?,?,?,?)", insert_buffer)
				insert_buffer = []

			total_p_count += len(insert_buffer)
			print("Paragraph Count: {:,d}".format(total_p_count), end="\r")

		if len(insert_buffer) >= 1:
			c.executemany("insert into data values (?,?,?,?,?,?)", insert_buffer)

		del sections

		conn.commit()

		print("Calculating number of examples in test sets...")

		assert sum(test_set_sizes) < 1.0
		test_set_counts = {}  # sgs_id -> [(count1, count2, ...)]

		sql = """
			select
				sense_group_sense_id,
				count(*)
			from
				data
			group by
				sense_group_sense_id
		"""
		for sgs_id, count in c.execute(sql):
			assert count >= 14
			test_set_counts[sgs_id] = tuple(map(lambda p: int(round(count * p)), test_set_sizes))

		print("Dividing data into test sets...")

		sql = """
			select
				article_id,
				section_index,
				paragraph_index,
				sentence_index
			from
				data
			where
				sense_group_sense_id = ?
			order by
				random()
		"""

		sql2 = """
			update
				data
			set
				dataset = ?
			where
				sense_group_sense_id = ?
				and article_id = ?
				and section_index = ?
				and paragraph_index = ?
				and ifnull(sentence_index, -1) = ?
		"""

		update_buffer = []

		paragraphs_already_in_datasets = []
		for _ in range(len(test_set_sizes) + 1):  # train set is last item
			paragraphs_already_in_datasets.append(set())

		for sgs_id, test_counts in sorted(test_set_counts.items(), key=lambda x: x[0], reverse=True):
			all_test_paragraphs = []
			for _ in range(len(test_counts)):
				all_test_paragraphs.append([])

			for art_id, sec_idx, par_idx, sen_idx in c.execute(sql, (sgs_id,)):
				paragraph_id = (art_id, sec_idx, par_idx, sen_idx if sen_idx is not None else -1)

				existing_in_set_index = None
				for i in range(len(paragraphs_already_in_datasets)):
					if paragraph_id in paragraphs_already_in_datasets[i]:
						existing_in_set_index = i
						break

				if existing_in_set_index is not None:
					if existing_in_set_index < len(all_test_paragraphs):
						all_test_paragraphs[existing_in_set_index].append(paragraph_id)
					continue

				need_more = False
				for test_paragraphs, target_count in zip(all_test_paragraphs, test_counts):
					if len(test_paragraphs) < target_count:
						need_more = True
						break

				if not need_more:
					paragraphs_already_in_datasets[-1].add(paragraph_id)
					continue

				# Paragraph not yet in any data sets. Add to first free
				for test_paragraphs, target_count, paragraphs_in_set in zip(all_test_paragraphs, test_counts, paragraphs_already_in_datasets[:-1]):
					if len(test_paragraphs) < target_count:
						test_paragraphs.append(paragraph_id)
						paragraphs_in_set.add(paragraph_id)
						break

			if sum(map(len, all_test_paragraphs)) < sum(test_counts):
				print("Warning: Did not reach target example count for sgs id", sgs_id)

			for i in range(len(all_test_paragraphs)):
				dataset_index = i + 1
				test_paragraphs = all_test_paragraphs[i]

				for art_id, sec_idx, par_idx, sen_idx in test_paragraphs:
					update_buffer.append(
						(
							dataset_index,
							sgs_id,
							art_id,
							sec_idx,
							par_idx,
							sen_idx
						)
					)

				if len(update_buffer) >= 5000:
					c.executemany(sql2, update_buffer)
					update_buffer = []

		if len(update_buffer) >= 1:
			c.executemany(sql2, update_buffer)

		c.execute("update data set dataset = 0 where dataset is null")

		print("Cleaning up and committing changes...")
		conn.executescript("vacuum;")

		conn.commit()
		conn.close()

	@staticmethod
	def worker_task(page_queue: mp.Queue, sql_queue: mp.Queue, paragraph_queue: mp.Queue):
		while True:
			page = page_queue.get()
			if page is None:
				break

			sql_queue.put(
				(
					"insert into 'articles' values (?,?,?,?,?,?)",
					[
						(
							page.id,
							normalize_page_title(page.title),
							group_title(page.title),
							page.redirect_article_title,  # already normalized
							page.redirect_section_id,  # already normalized
							0
						)
					]
				)
			)

			# Extract section info and paragraphs
			section_sql_values = []
			section_id_sql_values = []
			for section in page.sections:
				# Section info
				section_sql_values.append(
					(
						page.id,
						section.index,
						section.parent_index,
						section.title
					)
				)

				for id_string in section.ids:
					section_id_sql_values.append(
						(
							page.id,
							section.index,
							id_string
						)
					)

				# Paragraphs
				for paragraph_index in range(len(section.paragraphs)):
					paragraph = section.paragraphs[paragraph_index]

					links = []
					for link in paragraph.links:
						link_info = (
							link.range[0],
							link.range[1],
							link.linked_article_title,
							link.linked_section_id,
							link.title
						)
						links.append(link_info)

					paragraph_info = (
						page.id,
						section.index,
						paragraph_index,
						paragraph.text,
						links
					)

					paragraph_queue.put(paragraph_info)

			sql_queue.put(("insert into 'temp_sections' values (?,?,?,?)", section_sql_values))

		paragraph_queue.put(None)

	@staticmethod
	def tokenize_task(paragraph_queue: mp.Queue, tokens_queues: List[mp.Queue], links_queue: mp.Queue, count_queue: mp.Queue, classpath: str):
		number_of_tokens_queues = len(tokens_queues)

		properties = {
			"annotators": "tokenize,ssplit,pos,lemma",
			"tokenize.options": "untokenizable=noneKeep,invertible=true,ptb3Escaping=false",
			"tokenize.language": "en"
		}

		with CoreNlpBridge(classpath, properties, process_count=1) as corenlp:
			while True:
				paragraph_info = paragraph_queue.get()
				if paragraph_info is None:
					break

				page_id, section_index, paragraph_index, paragraph_text, links = paragraph_info

				try:
					sentences = corenlp.tokenize_text(paragraph_text)
				except CoreNlpBridge.TokenizationError:
					continue

				if len(sentences) == 0:
					print("Skipped paragraph without sentences!", paragraph_text)
					continue

				token_count = sum(map(len, sentences))
				if token_count < 5:
					# print("Skipped paragraph because it contains less than 5 tokens!", paragraph_text)
					continue

				section_key = (page_id, section_index)
				count_queue.put(section_key)

				# Map links to tokens
				sentence_ends = list(map(lambda s: s[-1].end, sentences))
				link_sentence_index = -1
				current_sentence_end = -1

				link_infos = []

				for link in sorted(links, key=lambda l: l[0]):
					link_begin = link[0]

					while link_begin >= current_sentence_end:
						link_sentence_index += 1
						current_sentence_end = sentence_ends[link_sentence_index]

					link_info = (
						page_id,
						section_index,
						paragraph_index,
						link_sentence_index
					) + link  # begin, end, linked_article_title, linked_section_id, link_title
					link_infos.append(link_info)

				links_queue.put(link_infos)

				token_infos = []
				for sentence_index in range(len(sentences)):
					sentence = sentences[sentence_index]
					for token in sentence:
						token_info = (
							page_id,
							section_index,
							paragraph_index,
							sentence_index,
							token.start,
							token.end,
							token.value,
							token.pos,
							token.lemma,
							token.before,
							token.after
						)
						token_infos.append(token_info)

				tokens_queues[page_id % number_of_tokens_queues].put(token_infos)

	@staticmethod
	def count_task(sql_queue: mp.Queue, count_queue: mp.Queue):
		section_counts = {}  # (article_id, section_index) -> count

		while True:
			section_key = count_queue.get()
			if section_key is None:
				break

			section_counts[section_key] = section_counts.get(section_key, 0) + 1

		sql_queue.put("DROP TABLE IF EXISTS 'section_paragraph_counts'")
		sql_queue.put("""
				CREATE TABLE "section_paragraph_counts" (
					"article_id" INTEGER,
					"section_index" INTEGER,
					"paragraph_count" INTEGER
				)
				""")

		sql = "insert into section_paragraph_counts values (?,?,?)"
		sql_buffer = []
		for key, count in section_counts.items():
			article_id, section_index = key
			sql_buffer.append((article_id, section_index, count))

			if len(sql_buffer) >= 500:
				sql_queue.put((sql, sql_buffer))
				sql_buffer = []

		if len(sql_buffer) >= 1:
			sql_queue.put((sql, sql_buffer))

		sql_queue.put("""
			CREATE UNIQUE INDEX section_paragraph_counts_index ON section_paragraph_counts (article_id, section_index);
		""")

	@staticmethod
	def db_task(db_path: str, sql_queue: mp.Queue):
		conn = sqlite3.connect(db_path)
		c = conn.cursor()

		while True:
			sql = sql_queue.get()
			if sql is None:
				break

			if isinstance(sql, str):
				c.execute(sql)
			else:
				sql, values = sql
				if len(values) == 1:
					c.execute(sql, values[0])
				else:
					c.executemany(sql, values)

		conn.commit()
		conn.close()

	@staticmethod
	def file_task(data_path: str, input_queue: mp.Queue):
		def tuple_to_string(x):
			return str(x if x is not None else "").replace("\n", " ").replace("\t", " ")

		with gzip.open(data_path, "wb") as f:
			while True:
				data = input_queue.get()
				if data is None:
					break

				for item in data:
					line = "\t".join(map(tuple_to_string, item)) + "\n"
					f.write(line.encode("utf8"))
