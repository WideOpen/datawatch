# coding: utf-8


import tarfile
import xml.etree.ElementTree as ET
import datetime
import tqdm
import re
import sqlite3
import unicodedata
from contextlib import closing
import os

import logging
logging.basicConfig(filename='parse_papers.log', level=logging.DEBUG)


def innertext(el):
    return "".join(el.itertext())


def get_meta_tag(root, tagtype):
    els = root.findall(
        ".//article-meta//article-id[@pub-id-type='%s']" % tagtype)
    if len(els) == 0:
        els = root.findall(".//article-id[@pub-id-type='%s']" % tagtype)

    if len(els) > 1:
        logging.warning("Warning, more than one %s detected" %
                        tagtype, extra={"n": len(els)})
        return None
    elif len(els) == 1:
        return els[0].text
    else:
        return None


def get_pmid(root):
    return get_meta_tag(root, "pmid")


def get_pmc(root):
    return get_meta_tag(root, "pmc")


def get_doi(root):
    return get_meta_tag(root, "doi")


def get_title(root):
    els = root.findall(".//article-meta//title-group/article-title")
    if len(els) == 0:
        els = root.findall(".//title-group/article-title")

    if len(els) > 1:
        logging.warning("Warning, more than one title detected",
                        extra={"n": len(els)})
        return None
    elif len(els) == 1:
        return innertext(els[0])
    else:
        return None


def get_journal_nlm(root):
    els = root.findall(".//journal-id[@journal-id-type='nlm-ta']")

    if len(els) > 1:
        logging.warning("Warning, more than one NLM detected",
                        extra={"n": len(els)})
        return None
    elif len(els) == 1:
        return innertext(els[0])
    else:
        return None


def nametotext(el):
    return ", ".join(map(innertext, el))


def get_authors(root):
    authors = root.findall(
        ".//contrib-group/contrib[@contrib-type='author']/name")
    return map(nametotext, authors)


def parse_date(el, allow_month_only=False):
    if el is None:
        return None
    year = el.find("year")
    month = el.find("month")
    day = el.find("day")
    if allow_month_only and day is None:
        day = 1
    if year is None or month is None or day is None:
        return None
    try:
        year = int(innertext(year))
        month = int(innertext(month))
        if day != 1:
            day = int(innertext(day))
        return datetime.date(year, month, day)
    except Exception as e:
        logging.warning("parse_date exception", exc_info=e,
                        extra={"inner": innertext(el)})
        return None

    return ""


def get_date(root):
    result = parse_date(root.find(".//pub-date[@pub-type='epub']"))
    if result is not None:
        return result

    result = parse_date(root.find(".//pub-date[@pub-type='pmc-release']"))
    if result is not None:
        return result

    result = parse_date(root.find(".//pub-date[@date-type='pub']"))
    if result is not None:
        return result

    result = parse_date(
        root.find(".//pub-date[@pub-type='ppub']"), allow_month_only=True)
    if result is not None:
        return result

    tmp = root.findall(".//pub-date")
    logging.warning("Failed to parse date", extra={
                    "data": map(lambda x: x.attrib, tmp)})
    return None


def parse_file(data):
    root = ET.fromstring(data)

    result = {"date": get_date(root),
              "title": get_title(root),
              "authors": get_authors(root),
              "pmc": get_pmc(root),
              "pmid": get_pmid(root),
              "doi": get_doi(root),
              "journal": get_journal_nlm(root)}
    return result


gse_expr = re.compile("GSE[0-9]+")
srx_expr = re.compile("SRX[0-9]+")

all_accessions_regexps = [gse_expr, srx_expr]

def find_accs(text):
    result = []
    for r in all_accessions_regexps:
        result.extend(r.findall(text))
    return result

def process_tar_file(fn):
    tf = tarfile.open(fn, "r")
    all_results = []
    for m in tqdm.tqdm(tf):
        if m.isfile():
            data = tf.extractfile(m).read()
            all_accs = find_accs(data)
            if len(all_accs) == 0:
                continue
            result = parse_file(data)
            result["gses"] = list(set(all_accs))
            all_results.append(result)
    tf.close()
    return all_results


def normalize(s):
    if isinstance(s, unicode):
        s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore')
    s = s.replace("\n", " ").replace("  ", " ")
    return s


def setup_db(dbcon):
    dbcon.executescript("""
-- core tables
create table authors(authorid integer primary key, name text);
create table papers(paperid integer primary key, title text, doi text UNIQUE, pmid integer UNIQUE, pmc integer UNIQUE, published_on date, journal_nlm text);
create table datasets(acc text primary key, title text, first_public_on date, first_submitted_on date, pmid_ref integer);

create table authorof(authorid int not null, paperid int not null);
create table mentions(paperid int not null, acc text not null);


create index authors_name_idx on authors(name);
create index mentions_paperid_idx on mentions(paperid);
create index mentions_acc_idx on mentions(acc);
create index authorof_authorid_idx on authorof(authorid);
create index authorof_paperid_idx on authorof(paperid);

PRAGMA synchronous=OFF;
""")


def get_author_id(dbcon, author):
    author = normalize(author)
    with closing(dbcon.cursor()) as cur:
        cur.execute("SELECT authorid from authors where name=?", (author,))
        data = cur.fetchall()
        if len(data) == 0:
            cur.execute("INSERT INTO authors(name) VALUES (?)", (author,))
            data = cur.lastrowid

            # dbcon.commit()

            return data
        else:
            return data[0][0]


def try_find_paper_by_column(dbcon, paper, column):
    with closing(dbcon.cursor()) as cur:
        if paper[column] is not None:
            cur.execute("select paperid from papers where " +
                        column + "=?", (paper[column],))
            data = cur.fetchall()
            if len(data) > 0:
                return data[0][0]
            else:
                return None


def try_find_paper(dbcon, paper):
    i = try_find_paper_by_column(dbcon, paper, "pmc")
    if i is not None:
        return i

    i = try_find_paper_by_column(dbcon, paper, "pmid")
    if i is not None:
        return i

    return try_find_paper_by_column(dbcon, paper, "doi")


def try_insert_paper(dbcon, paper):
    paperid = try_find_paper(dbcon, paper)
    if paperid is not None:
        return paperid

    # need to insert
    authorids = map(lambda x: get_author_id(dbcon, x), paper["authors"])

    with closing(dbcon.cursor()) as cur:
        cur.execute("insert into papers(title, doi, pmid, pmc, published_on, journal_nlm) values (?, ?, ?, ?, ?, ?)",
                    (paper["title"], paper["doi"], paper["pmid"], paper["pmc"], paper["date"], paper["journal"]))
        paperid = cur.lastrowid

        for aid in authorids:
            cur.execute(
                "insert into authorof(authorid, paperid) values (?, ?)", (aid, paperid))

        for gse in paper["gses"]:
            cur.execute(
                "insert into mentions(paperid, acc) values (?, ?)", (paperid, gse))
    # dbcon.commit()
    return paperid


def process_tar_to_db(dbcon, fn):
    print "Processing", fn
    all_results = process_tar_file(fn)
    print "Trying to add", len(all_results), "papers to the database"
    for res in tqdm.tqdm(all_results):
        try_insert_paper(dbcon, res)
    dbcon.commit()


def main():
    dbcon = sqlite3.connect("data/odw.sqlite")
    setup_db(dbcon)

    basedir = "rawdata/"
    files = os.listdir(basedir)
    for fn in files:
        if fn.endswith("tar.gz"):
            process_tar_to_db(dbcon, basedir + fn)

    dbcon.close()
    print "Done"

if __name__ == "__main__":
    main()
