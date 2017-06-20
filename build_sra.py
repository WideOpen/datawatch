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
import json

import GEOCacher


sraDB = sqlite3.connect("metadata/SRAmetadb.sqlite")

dataDB = sqlite3.connect("data/odw.sqlite")

def get_srx(srx):
    return sraDB.execute("select * from sra where experiment_accession=?", (srx,)).fetchall()

# %%


SRA_accs = dataDB.execute("select distinct acc from mentions where acc like 'SRX%'").fetchall()
SRA_accs = map(lambda x: x[0], SRA_accs)





import urllib2, json, requests

URL_esearch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
URL_efetch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

def get_data_about_accessions(db, accs):
    req_str = "db=" + db + "&retmode=json&retmax=50000&usehistory=y&term=" + "+OR+".join(map(lambda x: x + "[accn]", accs))
    resp = requests.post(URL_esearch, data=req_str)
    jdata = json.loads(resp.content)["esearchresult"]
    qkey = jdata["querykey"]
    webenv = jdata["webenv"]

    resp2 = requests.get(URL_efetch, {"db" : db, "query_key" : qkey, "webenv" : webenv, "email" : "grechkin@cs.washington.edu"})

    return resp2.content

def srx_not_public(srx):
    resp = requests.get(URL_esearch, {"email" : "grechkin@cs.washington.edu", "retmode" : "json", "db" : "sra", "term" : srx, "retmax" : 50, "usehistory" : "y"})

    jdata = json.loads(resp.content)["esearchresult"]
    qkey = jdata["querykey"]
    webenv = jdata["webenv"]

    resp2 = requests.get(URL_efetch, {"db" : "sra", "query_key" : qkey, "webenv" : webenv, "email" : "grechkin@cs.washington.edu"})
    return (srx + " is not public") in resp2.content.decode("utf-8")

#SRA_overdue1 = filter(lambda x: len(get_srx(x)) == 0, SRA_accs)
#SRA_overdue2 = filter(srx_not_public, SRA_overdue1)


def srx2url(srx):
    return "https://www.ncbi.nlm.nih.gov/sra/?term=" + srx


def pmid2url(pmid):
    return "https://www.ncbi.nlm.nih.gov/pubmed/" + pmid

def doi2url(doi):
    return "http://dx.doi.org/" + doi


def format_doi(doi):
    return """<a href="%s">%s</a>""" % (doi2url(doi), doi)



def format_srx(srx):
    return """<a href="%s">%s</a>""" % (srx2url(srx), srx)


def format_paper(pmid):
    return """<a href="%s">paper</a>""" % (pmid2url(pmid))

import urllib2
def check_is_removed(srx):
    srxurl = srx2url(srx)
    data = urllib2.urlopen(srxurl).read()
    return "Record is removed" in data


#SRA_overdue2_removed = filter(check_is_removed, SRA_overdue2)


#SRA_overdue2_kept = np.setdiff1d(SRA_overdue2, SRA_overdue2_removed)


SRA_overdue = []
for x in tqdm.tqdm(SRA_accs):
    if (len(get_srx(x)) == 0) and srx_not_public(x) and (not (check_is_removed(x))):
        SRA_overdue.append(x)


def get_paper(srx):
    return dataDB.execute("select * from papers, mentions where acc=? and mentions.paperid=papers.paperid", (srx,)).fetchall()


def get_df(accs):
    title = []
    doi = []
    published_on = []
    journal = []
    for srx in accs:
        paper = get_paper(srx)[0]
        title.append(paper[1])
        doi.append(paper[2])
        published_on.append(paper[5])
        journal.append(paper[6])
    df = pd.DataFrame({"srx" : accs, "title" : title, "doi" : doi, "published_on" : published_on, "journal" : journal})
    df = df[["srx", "published_on", "journal", "doi", "title"]]
    return df.sort_values(by="published_on")



tracking_script = """
<script>
  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');

  ga('create', 'UA-93388605-1', 'auto');
  ga('send', 'pageview');

</script>
"""



def update_html(df, sradb_timestamp):
    pd.set_option('display.max_colwidth', -1)
    table_html = df.to_html(formatters={
                            "doi": format_doi, "srx": format_srx}, escape=False, index=False, justify="left", classes="table table-striped table-bordered")

    html_template_str = unicode(open("sra_template.html").read())

    n_overdue = df.shape[0]

    final_html = html_template_str.format(date_updated=datetime.date.today(), sradb_timestamp=sradb_timestamp,
                                          n_overdue=n_overdue, table_html=table_html, tracking_script=tracking_script)

    with open("docs/sra.html", "w") as f:
        f.write(final_html.encode("utf-8"))



SRATIMESTAMP = sraDB.execute("select value from metaInfo where name='creation timestamp'").fetchall()[0][0]


def prepare_data_json(df_private, meta_timestamp, update_date):
    result = dict()
    result["meta_timestamp"] = meta_timestamp
    result["update_date"] = update_date
    result["data"] = [row[1].to_dict() for row in df_private.iterrows()]
    json.dump(result, open("private_sra.json", "w"))


df = get_df(SRA_overdue)
update_html(df, SRATIMESTAMP)
prepare_data_json(df, SRATIMESTAMP, str(datetime.date.today()))
