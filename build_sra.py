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
import sys; sys.path.append("/hdd2/ODW/")
import os
os.chdir("/hdd2/ODW")
import GEOCacher


sraDB = sqlite3.connect("metadata/SRAmetadb.sqlite")

dataDB = sqlite3.connect("data/odw.sqlite")

def get_srx(srx):
    return sraDB.execute("select * from sra where experiment_accession=?", (srx,)).fetchall()

# %%


SRA_accs = dataDB.execute("select distinct acc from mentions where acc like 'SRX%'").fetchall()

possible_overdue = filter(lambda x: len(get_srx(x[0])) == 0, SRA_accs)
