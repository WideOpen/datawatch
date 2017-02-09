import sqlite3
import urllib2
import zlib


STR_NOT_FOUND = "Could not find a public or private accession"
STR_DELETED = "was deleted by the GEO staff"
STR_PRIVATE = "is currently private and is scheduled to be released"


CREATE_SCRIPT = """
create table if not exists geo_pages(gse text, checked_date real, value blob);
create index if not exists geo_pages_gse_idx on geo_pages(gse);
"""


class GEOCacher(object):
    def __init__(self, db_filename):
        self.cache_db = sqlite3.connect(db_filename)
        self.cache_db.executescript(CREATE_SCRIPT)

    def _gse2url(self, gse):
        return "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + gse

    def get_geo_page(self, gse, maxlag=7):
        sql = """select * from geo_pages where gse=? and
                        checked_date + ? > julianday(datetime('now')) order by checked_date desc limit 1"""
        cur = self.cache_db.execute(sql, (gse, maxlag))
        data = cur.fetchall()
        if len(data) > 0:
            return zlib.decompress(bytes(data[0][2]))
        
        try:
            data = urllib2.urlopen(self._gse2url(gse)).read()
        except Exception as e:
            print "Error while fetching data for GSE: ", gse
            print "URL: ", self._gse2url(gse)
            print e
            raise e
        data_compr = zlib.compress(data)
        self.cache_db.execute("insert into geo_pages(gse, checked_date, value) values (?, julianday(datetime('now')), ?)", (gse, sqlite3.Binary(data_compr)))
        self.cache_db.commit()
        return data

    def check_gse_data(self, data):
        if STR_PRIVATE in data:
            return "private"
        elif STR_DELETED in data or STR_NOT_FOUND in data:
            return "missing"
        else:
            return "present"

    def check_gse_cached(self, gse, maxlag=7, skip_present=True):
        if skip_present:
            old_data = self.get_geo_page(gse, 365*20)
            if self.check_gse_data(old_data) == "present":
                return "present"
        data = self.get_geo_page(gse, maxlag)
        return self.check_gse_data(data)
