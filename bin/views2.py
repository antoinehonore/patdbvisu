import argparse
from src.utils import get_engine, get_dbcfg, read_query_file, clin_tables, all_data_tables, run_select_queries, get_size_interval, pidprint
import pandas as pd
import sys
from src.utils import get_colnames, get_size_patient, get_size_interval, define_col_db_size_query, get_colnames

dbcfg = get_dbcfg("cfg/db.cfg")

engine = get_engine(verbose=False, **dbcfg)

parser = argparse.ArgumentParser()
parser.add_argument("--drop", dest='drop', action='store_const',
                    const=True, default=False,
                    help='Only drop all views.')

import plotly.graph_objects as go
import plotly.express as px

import numpy as np


def get_med_str(dd, digits=3) -> list:
    """Compute medians for each of the scores(i.e. columns) in the input `pd.DataFrame`."""
    return [str(x) for x in np.round(dd.median(), digits).values]


def get_q_str(dd, digits=3, short=False) -> list:
    """Format the inter-quartile range for each score."""
    tmp_quartile = tuple(np.round(dd.quantile([0.25, 0.75]).values, digits))
    Q1, Q3 = tuple(tmp_quartile)
    if short:
        q_str = ["({})".format(round(q3-q1, digits)) for q1, q3 in zip(Q1, Q3)]
    else:
        q_str = ["({}-{})".format(q1, q3) for q1, q3 in zip(Q1, Q3)]
    return q_str


def join_str(*args):
    """Iteratively join elements of lists of `str`.
     The lists must all be the same size."""
    return [[" ".join([a, b]) for a, b in zip(*args)]]


def format_desc(dd, dtype=np.float64, digits=3, short=False):
    return join_str(get_med_str(dd.astype(dtype), digits=digits), get_q_str(dd.astype(dtype), short=short, digits=digits))


all_data_tables.pop(0)


from multiprocessing import Pool
from functools import partial
from datetime import datetime


def g(x, con=None):
    return pd.DataFrame.from_dict(get_size_interval(x, con), orient="index").T


if __name__ == "__main__":
    args = parser.parse_args()
    thepatid = "c656cb9d57076c8b57614e4141dd90f4d36580c0ae47aeda86098def5701d7dc"
    l = []
    fname = "/home/anthon@ad.cmm.se/owncloud.ki/PhD/documents/people/students/2022carolin/bool_data.sql"

    s = read_query_file()

    with engine.connect() as con:
        ilist = get_pat_intervals(thepatid, con)

        f = partial(g, con=con)
        start_ = datetime.now()
        out = list(map(f, ilist))
        print((datetime.now() - start_).total_seconds()/len(out), "sec/interv")



