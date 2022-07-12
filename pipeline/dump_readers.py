"""Beam transforms for reading from various dump files."""

import re
from itertools import islice
from typing import Optional

import apache_beam as beam
from smart_open import open as smart_open, register_compressor

def smart_line_reader(path: str, max_lines: Optional[int] = None):
    with smart_open(path, 'rb') as f:
        for i, line in enumerate(f):
            yield line
            if max_lines and (i >= max_lines):
                return

INSERT_PATTERN=re.compile(rb'INSERT INTO `.*` VALUES')


# Based on the schema at 
# https://www.mediawiki.org/wiki/Manual:Categorylinks_table
#
# See unit tests for example data. 
CATEGORYLINKS_ROW_PATTERN=re.compile(
        rb'\('
        rb'(?P<cl_from>\d+),'
        rb"'(?P<cl_to>.*?)',"
        rb"'(?P<cl_sortkey>.*?)',"
        rb"'(?P<cl_timestamp>.*?)',"
        rb"'(?P<cl_sortkey_prefix>.*?)',"
        rb"'(?P<cl_collation>.*?)',"
        rb"'(?P<cl_type>.*?)'"
        rb'\)'
        )


def parse_categorylinks_line(line: bytes):
    if not re.match(INSERT_PATTERN, line):
        return
    for match in re.finditer(CATEGORYLINKS_ROW_PATTERN, line):
        yield match.group('cl_to')


@beam.ptransform_fn
def CategorylinksDumpReader(_, path: str, max_lines: Optional[int] = None):
    """Reader for MediaWiki `categorylinks` table MySQL dump files.

    This is hacky as it assumes the schema / dumpfile format will not
    change. Processing the dump file directly avoids the time-consuming
    step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Categorylinks_table

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _ | 'ReadCategorylinksDumpFile' >> beam.Create(
            smart_line_reader(path, max_lines))

    titles = lines | 'ParseCategorylinksDumpLine' >> beam.FlatMap(
            parse_categorylinks_line)

    return titles


# Based on the schema at 
# https://www.mediawiki.org/wiki/Manual:Page_table
#
# See unit tests for example data. 
PAGE_ROW_PATTERN=re.compile(
        rb'\('
        rb'(?P<page_id>\d+),'
        rb'(?P<page_namespace>\d+),'
        rb"'(?P<page_title>.*?)',"
        rb"(?P<page_restrictions>.*?),"
        rb"(?P<page_is_redirect>\d+),"
  		rb"(?P<page_is_new>.*?),"
  		rb"(?P<page_random>.*?),"
  		rb"(?P<page_touched>.*?),"
  		rb"(?P<page_links_updated>.*?),"
  		rb"(?P<page_latest>.*?),"
  		rb"(?P<page_len>.*?),"
  		rb"(?P<page_content_model>.*?),"
  		rb"(?P<page_lang>.*?)"
        rb'\)'
        )


def parse_page_line(line: bytes):
    if not re.match(INSERT_PATTERN, line):
        return
    for match in re.finditer(PAGE_ROW_PATTERN, line):
        yield (int(match.group('page_id')), match.group('page_title'))


@beam.ptransform_fn
def PageDumpReader(_, path: str, max_lines: Optional[int] = None):
    """Reader for MediaWiki `page` table MySQL dump files.

    This is hacky as it assumes the schema / dumpfile format will not
    change. Processing the dump file directly avoids the time-consuming
    step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Page_table

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _ | 'ReadPageDumpFile' >> beam.Create(
            smart_line_reader(path, max_lines))

    return lines | 'ParsePageDumpLine' >> beam.FlatMap(
            parse_page_line)

