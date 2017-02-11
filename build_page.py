import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import datetime
import seaborn as sns
from dateutil.parser import parse
import tqdm
import sqlite3
import re
import argparse

import GEOCacher


def combine_old_private(df_old, df_private):
    df1 = df_old[["gse", "first_mentioned", "released"]]
    df2 = df_private[["gse"]]
    df2.loc[:, "first_mentioned"] = df_private["published_on"]
    df2.loc[:, "released"] = [None] * (df_private.shape[0])
    return pd.concat([df1, df2])


prepare_temp_tables = """
CREATE temp table  first_mention  as 
    select m.gse, p.paperid 
        from mentions m, papers p
           where m.paperid = p.paperid and p.published_on = 
                (select min(published_on) from papers p, mentions m0 where p.paperid=m0.paperid and m0.gse = m.gse);

CREATE index temp.first_mention_gse_idx on first_mention(gse);

CREATE temp table gse_times  AS 
  select ds.gse, ds.first_submitted_on as submitted, ds.first_public_on as released, p.published_on as first_mentioned, ds.title, m.paperid as first_paper
  from datasets ds 
        left join first_mention m on m.gse = ds.gse
        left join papers p on p.paperid = m.paperid;
"""


def load_dataframes(maxlag):
    print "Loading data..."
    data_db = sqlite3.connect("data/odw.sqlite")

    cache = GEOCacher.GEOCacher("data/cache.sqlite")
    data_db.executescript(prepare_temp_tables)

    query_released = """
    select gse, doi, papers.title, first_mentioned, released, journal_nlm from gse_times, papers 
            where
            papers.paperid = gse_times.first_paper
    """
    df_released = pd.read_sql_query(query_released, data_db)

    query_private = """select distinct gse, published_on, journal_nlm as journal, 
                    doi, title from mentions, papers 
                        where gse not in (select gse from datasets) and mentions.paperid=papers.paperid
                        order by published_on asc"""
    df_missing = pd.read_sql_query(query_private, data_db)

    skip_gses = map(lambda x: x.split()[0], open("whitelist.txt").readlines())

    print "Double-checking missing GSE's using NCBI website..."

    statuses_released = []
    for (i, gse) in enumerate(tqdm.tqdm(df_released.gse)):
        if df_released.released[i] is None:
            status = cache.check_gse_cached(gse, maxlag=maxlag)
            statuses_released.append(False)
            if status == "private":  # append it to df_missing then
                # Index([u'gse', u'published_on', u'journal', u'doi', u'title'], dtype='object')
                df_missing = df_missing.append({"gse": gse, "published_on": df_released.first_mentioned[i], "journal": df_released.journal_nlm[i],
                                                "doi": df_released.doi[i], "title": df_released.title[i]}, ignore_index=True)
                print "Weird GSE: ", gse

        else:
            statuses_released.append(True)

    nonreleased = np.nonzero(np.invert(np.array(statuses_released)))[0]
    # print "Missing GSEs that are mentioned in GEOMetadb :",
    # df_released.gse[nonreleased]
    df_released = df_released.ix[np.nonzero(statuses_released)[0]]

    today_str = str(datetime.date.today())
    statuses = []
    for (i, gse) in enumerate(tqdm.tqdm(df_missing.gse)):
        if gse in skip_gses:
            statuses.append("skip")
        else:
            status = cache.check_gse_cached(gse, maxlag=maxlag)
            if status == "present":
                data = cache.get_geo_page(gse, maxlag=99999)
                reldate = re.search("Public on (.*)<", data)
                if reldate is None:
                    print "Failed to extract date for ", gse
                    reldate = today_str
                else:
                    reldate = reldate.group(1)
                df_released = df_released.append({"doi": df_missing.gse[i], "gse": gse, "title": df_missing.title[i], "journal_nlm": df_missing.journal[
                                                 i], "first_mentioned": df_missing.published_on[i],  "released": reldate}, ignore_index=True)
            statuses.append(status)

    df_private = df_missing.ix[np.array(statuses) == "private"]
    df_private = df_private.sort_values("published_on")

    cur = data_db.execute(
        "select value from metadata where name = 'GEOmetadb timestamp'")
    meta_timestamp = cur.fetchone()[0]

    return df_private, df_released, meta_timestamp


def get_hidden_df(df):
    df = df.copy()
    oneday = datetime.timedelta(1)
    onemonth = datetime.timedelta(30)
    x = []
    y = []
    c = datetime.date.today() - oneday
    mentioned = np.array(map(lambda x: parse(x).date(), df.first_mentioned))
    filldate = (datetime.datetime.today() + datetime.timedelta(1)).date()
    public = np.array(map(lambda x: parse(x).date(),
                          df.released.fillna(str(filldate))))
    while c >= datetime.date(2008, 1, 1):
        mask1 = mentioned < c
        mask2 = public > c + oneday
        x.append(c)
        y.append(np.count_nonzero(mask1 & mask2))
        c -= onemonth

    print "Current overdue: ", y[0]

    return pd.DataFrame({"date": x, "overdue": y})


def update_graph(df):
    dff = get_hidden_df(df)

    sns.set_style("white")
    sns.set_style("ticks")

    sns.set_context("talk")
    dff.plot("date", "overdue", figsize=(7, 4), lw=3)
    onemonth = datetime.timedelta(30)
    plt.xlim(dff.date.min(), dff.date.max()+onemonth)
    plt.ylabel("Overdue dataset")
    plt.xlabel("Date")
    plt.savefig("docs/graph.png")


def gse2url(gse):
    return "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse


def doi2url(doi):
    return "http://dx.doi.org/" + doi


def format_gse(gse):
    return """<a href="%s">%s</a>""" % (gse2url(gse), gse)


def format_doi(doi):
    return """<a href="%s">%s</a>""" % (doi2url(doi), doi)


def update_html(df, metadb_timestamp):

    pd.set_option('display.max_colwidth', -1)
    table_html = df.to_html(formatters={
                            "doi": format_doi, "gse": format_gse}, escape=False, index=False, justify="left")

    html_template_str = unicode(open("output_template.html").read())

    n_overdue = df.shape[0]

    final_html = html_template_str.format(date_updated=datetime.date.today(), metageo_timestamp=metadb_timestamp,
                                          n_overdue=n_overdue, table_html=table_html)

    with open("docs/index.html", "w") as f:
        f.write(final_html.encode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description='Build a page for datawatch')

    parser.add_argument("--maxlag", default=7)
    args = parser.parse_args()

    df_private, df_released, meta_timestamp = load_dataframes(args.maxlag)
    combined_df = combine_old_private(df_released, df_private)
    print "Currently missing entries in GEOMetadb: ", df_private.shape[0]

    # combined_df.to_csv("output/combined_data.csv")

    update_html(df_private, meta_timestamp)
    update_graph(combined_df)

if __name__ == "__main__":
    main()
