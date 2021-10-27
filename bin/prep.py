from unidecode import unidecode
import re
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils import gdate, date_fmt, get_engine, get_dbcfg, pidprint, mon_sig_name_fix
import sys
import os
from sqlalchemy import create_engine
import hashlib
import base64
import zlib


from functools import partial

def mondata_test(bname):
    return bname.startswith("LF__") or bname.startswith("HF__")

def tkdata_test(bname):
    return "_takecare" in bname

def clindata_test(bname):
    return parse("{}_read_{}.csv", bname)

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


def register_change_num(d, ichunk=None,data_col=None):
    thevars = [s for s in list(d) if s in data_col]
    out = {}

    for thevar in thevars:
        chg = ((d[thevar] != d[thevar].shift(1)).cumsum() - 1)
        all_chg = chg.unique()
        registered = []
        for i in range(len(all_chg)):
            # l.append((d[chg == all_chg[i]]["respirator"].unique()[0], d[chg == all_chg[i]]["Tid"].max()-d[chg == all_chg[i]]["Tid"].min()))
            registered.append("__".join(list(map(str, (d[chg == all_chg[i]][thevar].unique()[0],
                                                       datetime.strftime(d[chg == all_chg[i]]["date"].min(), date_fmt))))))
        out[thevar] = "___".join([r for r in registered if not "nan" in r])

    out_df = pd.DataFrame(columns=list(out.keys()), data=np.array(list(out.values())).reshape(1, -1), index=[ichunk])
    return out_df


def register_values(d,ichunk=None,data_col=None):
    thevars = [s for s in list(d) if s in data_col]
    out = {}

    for thevar in thevars:
        chg = d[["date"]+[thevar]][d[thevar].notna()]
        out[thevar] = "___".join(["__".join([str(ll[1]),datetime.strftime(ll[0],date_fmt)]) for ll in chg.values])

    out_df = pd.DataFrame(columns=list(out.keys()), data=np.array(list(out.values())).reshape(1, -1), index=[ichunk])
    out_df.replace({"":np.nan},inplace=True)
    return out_df

def aggregate_clin_data(d, ichunk):
    l = list(d)

    data_col = [ll for ll in l if (ll != "date") and (ll != "tkid") and \
                                  (ll != "local_id") and (ll != "lab_empty")\
                                    and (ll != "vatska_givet_empty")]

    out_data_col = data_col

    if ("fio2" in l) or ("ppeak" in l) or ('tempaxil' in l) or any("lab_" in ll for ll in l) or ("cirk_vikt" in l):
        out_tmp = register_change_num(d, ichunk=ichunk, data_col=data_col)
        out = pd.DataFrame(columns=out_data_col, data=out_tmp.values.reshape(1, -1), index=[ichunk])

    elif any("lm_givet" in ll for ll in l) or any("vatska" in ll for ll in l):
        out_tmp = register_values(d, ichunk=ichunk, data_col=data_col)
        out = pd.DataFrame(columns=out_data_col, data=out_tmp.values.reshape(1, -1), index=[ichunk])

    elif ("respirator" in l):
        out = register_change_resp(d, ichunk=ichunk, data_col=data_col)

    return out


def register_change_resp(d,ichunk=None,data_col=None):
    chg = ((d["respirator"] != d["respirator"].shift(1)).cumsum() - 1)
    all_chg = chg.unique()
    l = []
    for i in range(len(all_chg)):
        # l.append((d[chg == all_chg[i]]["respirator"].unique()[0], d[chg == all_chg[i]]["Tid"].max()-d[chg == all_chg[i]]["Tid"].min()))
        l.append((d[chg == all_chg[i]]["respirator"].unique()[0], d[chg == all_chg[i]]["date"].min()))
    out = {"{}_{}".format("respirator", k): "" for k in list(set([ll[0] for ll in l]))}
    for ll in l:
        out["{}_{}".format("respirator", ll[0])] += datetime.strftime(ll[1], date_fmt) + "__"
    out_df = pd.DataFrame(columns=list(out.keys()), data=np.array(list(out.values())).reshape(1, -1), index=[ichunk])

    return out_df


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
        .replace("sepsis__ruled__out", "sro") \
        .replace("no__note__on__clinical__event", "no__notes")\
        .replace("days__with__antibiotics", "days__antibio") \


def read_summaries(fname):
    d = pd.read_csv(fname,
                    sep=";",
                    names=["monid", "signame", "start", "end", "duration", "gap_str"],
                    parse_dates=True,
                    infer_datetime_format=True)
    return d


def prep(args):
    infname = args.i
    outfname = args.o

    rx = re.compile('\W+')
    if infname.endswith(".xlsx"):
        df = pd.read_excel(infname)
    else:
        df = read_summaries(infname)

    df.columns = [rx.sub(' ', unidecode(s.strip())).strip().lower().replace(" ", "_") for s in df.columns]
    if "monitor_meta" in os.path.basename(infname):
        df["bedlabel"] = df["signame"].apply(lambda s:s.split("__")[-2])
        df["clinicalunit"] = df["signame"].apply(lambda s: s.split("__")[-1])
        df["signame"] = df["signame"].apply(lambda s: "__".join(s.split("__")[:-2]))
        df.rename(columns={"personnummer":"ids__uid", "start":"thestart","end":"theend"},inplace=True)
        df=df.replace(";", "", regex=True)
        df = create_idx(df,
                        ["ids__uid", "monid", "signame","bedlabel","clinicalunit", "thestart","theend"],
                        "ids__mondata", "mondata__raw"
                        )

        df = df[['monid','ids__uid', 'signame', 'bedlabel', 'clinicalunit', 'thestart', 'theend', 'duration', 'gap_str',"ids__mondata", "mondata__raw"]]

    df.replace({".": np.nan, "-": np.nan}).to_csv(outfname, sep=";", index=False)


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

    # Encode the interval info
def create_idx(df,interval_characterization,keyname,keyraw):

    hash_fun = lambda s: hashlib.sha256(s.encode("utf8")).hexdigest()

    ids__interval__data = df[interval_characterization].applymap(lambda x: str(x)).values
    df[keyraw] = [";".join(l) for l in ids__interval__data]
    df[keyname] = [hash_fun(";".join(l)) for l in ids__interval__data]
    return df



def chunk(args):
    fname = args.i
    outfname = args.o
    period = args.p
    date_col = args.date
    id_col = args.id
    map_tbl = args.maptbl

    bname = os.path.basename(fname)

    if mondata_test(bname):
        s = os.path.basename(outfname).replace(".csv", "").split("__")
        signame = "__".join(s[:-2])
        bedlabel = s[-2]
        unitname = s[-1]

        if "antekt" in signame:
            pidprint("Ignoring", signame, flag="error")
            sys.exit(1)

        df = pd.read_csv(fname,
                         sep=";",
                         names=["date", "data"])
        df["local_id"] = os.path.basename(bname)
        id_col = "monid"
        map_tbl = "monitor_meta"

        agg_fun = partial(aggregate_mon_data, signame=signame, bedlabel=bedlabel,unitname=unitname)

    elif tkdata_test(bname):
        df = pd.read_csv(fname, sep=";")

        df.rename(columns={date_col: "date",
                           id_col: "local_id"},
                  inplace=True)

        df["key"] = make_tkevt_key(df)

        df = pd.concat([df[["local_id", "date", "extra"]],
                        pd.get_dummies(df["key"], prefix=args.cpref, prefix_sep="__")],
                        axis=1).astype(str)

        df = df.replace("0", np.nan)
        df = df.apply(procrow, axis=1)

        df.columns = list(map(format_tkevt_string, df.columns))
        agg_fun = aggregate_tk_data

    elif clindata_test(bname):
        df = pd.read_csv(fname, sep=";")
        id_col="clinid"
        map_tbl="overview"

        df.rename(columns={"Tid": "date", id_col: "local_id"},
                  inplace=True)

        print("")
        agg_fun = aggregate_clin_data

    if df.shape[0] == 0:
        pidprint("Empty input file", fname, flag="error")
        sys.exit(1)

    local_id = df.local_id.unique()[0]

    out = chunk_fun(df, agg_fun, local_id)

    LOG = {}
    dbcfg = get_dbcfg("cfg/db.cfg")

    engine = get_engine(verbose=False, **dbcfg)

    # Find the ids__uid
    with engine.connect() as con:
        tmp = pd.read_sql("select distinct ids__uid from {} where {} like \'%%{}%%\'".format(map_tbl, id_col, local_id), con)

    if tmp.shape[0] == 0:
        pidprint("local_id:{} not found in overview.".format(local_id), flag="error")
        sys.exit(1)

    elif tmp.shape[0] > 1:
        pidprint("local_id:{} found more than once in overview.".format(local_id), flag="error")
        sys.exit(1)

    ids__uid = tmp["ids__uid"].iloc[0]
    out["ids__uid"] = ids__uid
    out.drop(columns=["local_id"], inplace=True)

    out=create_idx(out,["ids__uid", "interval__start", "interval__end"], "ids__interval", "interval__raw")

    out.to_csv(outfname, sep=";", index=False)


parser = argparse.ArgumentParser(description="Longitudinal data are grouped in chunks.")
parser.add_argument("-i", type=str)
parser.add_argument("-o", type=str)
parser.add_argument("-id", type=str, default="tkid")
parser.add_argument("-date", type=str, default="date")
parser.add_argument("-p", type=str, default="days")
parser.add_argument("-cpref", type=str, default="tkevt", help="column prefix")
parser.add_argument("-maptbl", type=str, default="overview", help="table containing mapping local_id, ids__uid")

from parse import parse

if __name__ == "__main__":
    args = parser.parse_args()
    bname = os.path.basename(args.i)
    os.makedirs(os.path.dirname(args.o), exist_ok=True)
    if tkdata_test(bname) or mondata_test(bname) or clindata_test(bname):
        chunk(args)
    else:
        prep(args)

