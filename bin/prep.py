from unidecode import unidecode
import re
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils import gdate
import sys
import os
from sqlalchemy import create_engine
import hashlib
import base64
import zlib


def agg_tk(col):
    if col.shape[0]==0 or col.isna().all():
        return np.nan
    else:
        if col.name == "tkid":
            return col.values[0]
        elif col.name == "tkevt__extra":
            return "__".join([v for v in col.values if isinstance(v, str)])
        else:
            return "___".join([col.name+"@"+v for v in col.values.astype(str) if v!="nan"])


def aggregate_tk_data(d, ichunk):
    out = d.apply(agg_tk, result_type="reduce").to_frame()
    out = pd.DataFrame(columns=out.index, data=out.values.reshape(1,-1),index=[ichunk])
    return out

def aggregate_mon_data(d, ichunk, signame="here", bedlabel="unknown",unitname="unknown"):
    out = pd.DataFrame(columns=[signame], data=np.array([compress_chunk(d)]).reshape(1,-1),index=[ichunk])
    out["bedlabel"] = bedlabel
    out["unitname"] = unitname
    return out


def procrow(row):
    row[row == "1"] = row["date"]
    return row


def make_tkevt_key(data):
    return data["event"] + "/" + data["specificities"] + "/" + data["notes"]


def format_tkevt_string(s):
    return s.strip().lower().replace(",", "")\
        .replace("(", "").replace(".", "")\
        .replace(")", "").replace(" ", "__")\
        .replace("culture__negative__sepsis", "CNSepsis") \
        .replace("cardiorespiratory__system", "CRSystem") \
        .replace("staphylococcus", "staph")\
        .replace("no__note__on__clinical__event", "no__notes")\
        .replace("days__with__antibiotics", "d_w_antibio")


def prep(args):
    infname = args.i
    outfname = args.o

    rx = re.compile('\W+')

    df = pd.read_excel(infname)
    df.columns = [rx.sub(' ', unidecode(s.strip())).strip().lower().replace(" ", "_") for s in df.columns]
    df.replace(";", "", regex=True).replace({".": np.nan, "-": np.nan}).to_csv(outfname, sep=";", index=False)



def compress_chunk(d):
    if d.empty:
        c = compress_string(pd.DataFrame().to_csv(None, sep=";", index=False))
    else:
        c = compress_string(d.to_csv(None, sep=";", index=False))
    return c

def compress_string(s):
    return base64.b64encode(zlib.compress(s.encode("utf8"))).decode("utf8")


def chunk_fun(df, agg_fun, local_id):
    df["date"] = pd.to_datetime(df["date"])
    start_date = df["date"].min()
    end_date = df["date"].max()

    step = timedelta(days=1)
    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.max.time())

    n_full_intervals = (end_date - start_date) // step + 1
    if n_full_intervals > 0:
        intervals = [(start_date + i * step, start_date + (i + 1) * step) for i in range(n_full_intervals)]
    else:
        intervals = [(start_date, end_date)]

    out = pd.DataFrame()
    for i_interv, (start, end) in enumerate(intervals):
        chunk = df[(df["date"] >= start).values & (df["date"] < end).values]

        tmp_chunk = agg_fun(chunk, ichunk=i_interv + 1)

        tmp_chunk["interval__start"] = start
        tmp_chunk["interval__end"] = end
        if "date" in tmp_chunk.columns:
            tmp_chunk.drop(columns=["date"], inplace=True)

        if not ("local_id" in tmp_chunk.columns):
            tmp_chunk["local_id"] = local_id
        out = out.append(tmp_chunk, sort=True)

    first_cols = ["local_id", "interval__start", "interval__end"]
    out = out[first_cols + [s for s in out.columns if not (s in first_cols)]]
    return out


from functools import partial


def chunk(args):
    fname = args.i
    outfname = args.o
    period = args.p
    date_col = args.date
    id_col = args.id
    map_tbl = args.maptbl
    bname = os.path.basename(fname)

    if bname.startswith("LF__") or bname.startswith("HF__"):
        df = pd.read_csv(fname,
                         sep=";",
                         names=["date", "data"]
                         )
        df["local_id"] = os.path.basename(os.path.dirname(fname))
        id_col = "monid"
        map_tbl = "monitor_meta"
        s = bname.replace(".csv", "").split("__")
        signame = "__".join(s[:-2])
        bedlabel = s[-2]
        unitname = s[-1]

        agg_fun = partial(aggregate_mon_data, signame=signame, bedlabel=bedlabel,unitname=unitname)

    elif "_takecare" in bname:
        df = pd.read_csv(fname, sep=";")

        df.rename(columns={date_col: "date", id_col: "local_id"},
                  inplace=True)

        df["key"] = make_tkevt_key(df)

        df = pd.concat([df[["local_id", "date", "extra"]],
                        pd.get_dummies(df["key"], prefix=args.cpref, prefix_sep="__")],
                        axis=1).astype(str)

        df = df.replace("0", np.nan)
        df = df.apply(procrow, axis=1)

        df.columns = list(map(format_tkevt_string, df.columns))
        agg_fun = aggregate_tk_data

    local_id = df.local_id.unique()[0]

    out = chunk_fun(df, agg_fun, local_id)

    username = "anthon"
    passwd = "1234"
    schema = "public"

    engine = create_engine('postgresql://{}:{}@127.0.0.1:5432/patdb'.format(username, passwd))

    # Find the ids__uid
    with engine.connect() as con:
        tmp = pd.read_sql("select distinct ids__uid from {} where {} like \'%%{}%%\'".format(map_tbl, id_col, local_id), con)

    if tmp.shape[0] == 0:
        print(gdate(), "error", "local_id:{} not found in overview.".format(local_id), file=sys.stderr)
        sys.exit(1)

    elif tmp.shape[0] > 1:
        print(gdate(), "error", "local_id:{} found more than once in overview.".format(local_id), file=sys.stderr)
        sys.exit(1)

    ids__uid = tmp["ids__uid"].iloc[0]
    out["ids__uid"] = ids__uid
    out.drop(columns=["local_id"], inplace=True)

    hash_fun = lambda s: hashlib.sha256(s.encode("utf8")).hexdigest()
    interval_characterization = ["ids__uid", "interval__start", "interval__end"]
    ids__interval__data = out[interval_characterization].applymap(lambda x: str(x)).values
    out["interval__raw"] = [";".join(l) for l in ids__interval__data]
    out["ids__interval"] = [hash_fun(";".join(l)) for l in ids__interval__data]
    out.to_csv(outfname, sep=";", index=False)


parser = argparse.ArgumentParser(description="Longitudinal data are grouped in chunks.")
parser.add_argument("-i", type=str)
parser.add_argument("-o", type=str)
parser.add_argument("-id", type=str, default="tkid")
parser.add_argument("-date", type=str, default="date")
parser.add_argument("-p", type=str, default="days")
parser.add_argument("-cpref", type=str, default="tkevt", help="column prefix")
parser.add_argument("-maptbl", type=str, default="overview", help="table containing mapping local_id, ids__uid")


if __name__ == "__main__":
    args = parser.parse_args()
    bname = os.path.basename(args.i)
    os.makedirs(os.path.dirname(args.o), exist_ok=True)
    if ("_takecare.csv" in bname) or bname.startswith("HF__") or bname.startswith("LF__"):
        chunk(args)
    else:
        prep(args)

