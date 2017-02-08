# coding: utf-8

import sqlite3
import datetime
import dateutil.parser
import tqdm


def parse_date(s):
    s = s.replace("Public on ", "")
    return dateutil.parser.parse(s).date()


def get_date_by_query(metadb, query, gse):
    cur = metadb.cursor()
    cur.execute(query, (gse,))
    values = map(lambda x: parse_date(x[0]), cur.fetchall())
    cur.close()
    if len(values) > 0:
        return min(values)
    else:
        return None


def get_released_date(metadb, gse):
    query = "select distinct status from gsm, gse_gsm where gsm.gsm = gse_gsm.gsm and gse_gsm.gse = ?"
    return get_date_by_query(metadb, query, gse)


def get_submitted_date(metadb, gse):
    query = ("select distinct submission_date as date from gsm, gse_gsm where " +
             " gse_gsm.gsm = gsm.gsm and gse_gsm.gse = ? group by submission_date")
    return get_date_by_query(metadb, query, gse)


def get_pmid(metadb, gse):
    cur = metadb.cursor()
    cur.execute("select pubmed_id from gse where gse=?", (gse, ))
    data = cur.fetchall()
    cur.close()
    return data[0][0]


def get_title(metadb, gse):
    cur = metadb.cursor()
    cur.execute("select title from gse where gse=?", (gse, ))
    data = cur.fetchall()
    cur.close()
    return data[0][0]


def parse_metadb(metadb):

    cur = metadb.cursor()
    cur.execute("select gse from gse")
    all_gse = map(lambda x: x[0], cur.fetchall())

    all_results = {}
    for gse in tqdm.tqdm(all_gse):
        released = get_released_date(metadb, gse)
        submitted = get_submitted_date(metadb, gse)
        pmid = get_pmid(metadb, gse)
        title = get_title(metadb, gse)
        all_results[gse] = (submitted, released, pmid, title)

    return all_gse, all_results


def insert_data(dbcon, all_gse, all_results):
    cur = dbcon.cursor()
    for gse in tqdm.tqdm(all_gse):
        (submitted, released, pmid, title) = all_results[gse]
        cur.execute("SELECT * FROM datasets where gse=?", (gse,))
        data = cur.fetchall()
        if len(data) > 0:
            continue
        cur.execute("INSERT INTO datasets (gse, title, first_public_on, first_submitted_on, pmid_ref) VALUES (?, ?, ?, ?, ?)",
                    (gse, title, released, submitted, pmid))
    dbcon.commit()
    cur.close()

    added = 0
    not_found = []
    cur = dbcon.cursor()
    for gse in tqdm.tqdm(all_gse):
        (submitted, released, pmid, title) = all_results[gse]
        cur.execute("SELECT paperid from papers where pmid=?", (pmid,))
        data = cur.fetchall()
        if len(data) == 0:
            not_found.append(pmid)
            continue
        paperid = data[0][0]

        cur.execute(
            "SELECT * from mentions where gse=? and paperid=?", (gse, paperid))
        data = cur.fetchall()
        if len(data) == 0:
            added += 1
            cur.execute(
                "INSERT into mentions (gse, paperid) values (?, ?)", (gse, paperid))
    dbcon.commit()
    cur.close()

    not_found_pmids = filter(lambda x: x is not None, not_found)
    print "Not found: ", len(not_found_pmids)


def update_metadb_stamp(dbcon, metadb):
    dbcon.executescript(
        "create table if not exists metadata(name text unique, value text);")
    cur = metadb.execute(
        "select value from metaInfo where name='creation timestamp'")
    metaTimestamp = cur.fetchone()[0]

    dbcon.execute(
        "INSERT or replace into metadata(name, value) values ('GEOmetadb timestamp', ?);", (metaTimestamp, ))
    dbcon.commit()


def main():
    metadb = sqlite3.connect("rawdata/GEOmetadb.sqlite")
    dbcon = sqlite3.connect("data/odw.sqlite")

    print "Parsing metadb..."
    all_gse, all_results = parse_metadb(metadb)
    print "Inserting data..."
    insert_data(dbcon, all_gse, all_results)

    update_metadb_stamp(dbcon, metadb)
    dbcon.close()

if __name__ == "__main__":
    main()
