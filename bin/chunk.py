

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils import gdate
import sys,os
from sqlalchemy import create_engine
import hashlib


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


def main(args):
    fname = args.i
    outfname = args.o
    period = args.p
    date_col = args.date
    id_col = args.id

    df = pd.read_csv(fname, sep=";")
    df.rename(columns={date_col: "date", id_col: "local_id"},
              inplace=True)
    if not "extra" in df.columns:
        df["extra"] = ""


    df["key"] = make_tkevt_key(df)

    df = pd.concat([df[["local_id", "date", "extra"]],
                    pd.get_dummies(df["key"], prefix=args.cpref, prefix_sep="__")],
                    axis=1).astype(str)

    local_id = df.local_id.unique()[0]

    df = df.replace("0", np.nan)
    df = df.apply(procrow, axis=1)

    df.columns = list(map(format_tkevt_string, df.columns))
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
        tmp_chunk = aggregate_tk_data(chunk, ichunk=i_interv + 1)
        tmp_chunk["interval__start"] = start
        tmp_chunk["interval__end"] = end
        tmp_chunk.drop(columns=["date"], inplace=True)
        out = out.append(tmp_chunk, sort=True)

    first_cols = ["local_id", "interval__start", "interval__end"]
    out = out[first_cols + [s for s in out.columns if not (s in first_cols)]]

    username = "anthon"
    passwd = "1234"
    schema = "public"

    engine = create_engine('postgresql://{}:{}@127.0.0.1:5432/patdb'.format(username, passwd))

    # Find the ids__uid
    with engine.connect() as con:
        tmp = pd.read_sql("select ids__uid from overview where {} like \'%%{}%%\'".format(id_col, local_id), con)

    if tmp.shape[0] == 0:
        print(gdate(), "error", "local_id:{} not found in overview.".format(local_id), file=sys.stderr)
        sys.exit(1)

    elif tmp.shape[0] > 1:
        print(gdate(), "error", "local_id:{} found more than once in overview.".format(local_id), file=sys.stderr)
        sys.exit(1)

    ids__uid=tmp["ids__uid"].iloc[0]
    out["ids__uid"] = ids__uid
    out.drop(columns=["local_id"], inplace=True)

    hash_fun = lambda s: hashlib.sha256(s.encode("utf8")).hexdigest()
    interval_characterization = ["ids__uid", "interval__start", "interval__end"]
    ids__interval__data = out[interval_characterization].applymap(lambda x: str(x)).values
    out["ids__interval"] = [hash_fun(";".join(l)) for l in ids__interval__data]
    out.to_csv(outfname, sep=";", index=False)
    print("")



parser = argparse.ArgumentParser(description="Longitudinal data are grouped in chunks.")
parser.add_argument("-i", type=str)
parser.add_argument("-o", type=str)
parser.add_argument("-id", type=str, default="tkid")
parser.add_argument("-date", type=str, default="date")
parser.add_argument("-p", type=str, default="days")
parser.add_argument("-cpref", type=str, default="tkevt", help="column prefix")

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
