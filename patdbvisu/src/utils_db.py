from io import StringIO
import pandas as pd
from functools import partial
import numpy as np
import base64
import zlib
import os
from datetime import datetime
import sys
from sqlalchemy import create_engine


# Remove empty
def clean_sig(d):
    return d[d != None]


def read_passwd(username: str = "remotedbuser", root_folder: str = ".") -> str:
    """
    Read `username` password file from the `root_folder`.
    """
    with open(os.path.join(root_folder, "{}_dbfile.txt".format(username)), "r") as f:
        s = f.read().strip()
    return s


def get_engine(username: str = "remotedbuser", root_folder: str = ".", nodename: str = "client", schema=None, dbname:str="remotedb", verbose=False):
    """
    Get a database `sqlalchemy.engine` object for the user `username`, using ssl certificates specific for 'nodenames' type machines.
    For details about the database engine object see `sqlalchemy.create_engine`
    """

    passwd = read_passwd(username=username, root_folder=root_folder)
    connect_args = {}
    if username == "remotedbdata":
        connect_args = {'sslrootcert': os.path.join(root_folder, "root.crt"),
                        'sslcert': os.path.join(root_folder, "{}.crt".format(nodename)),
                        'sslkey': os.path.join(root_folder, "{}.key".format(nodename))}

    engine = create_engine('postgresql://{}:{}@127.0.0.1:5432/{}'.format(username, passwd,dbname),
                           connect_args=connect_args)
    with engine.connect() as con:
        if verbose:
            pidprint("Connection OK", flag="report")
        else:
            pass
    return engine


def run_query(s: str, engine, verbose=0) -> pd.DataFrame:
    """
    Runs the query specified as a string vs a db engine.

    Inputs:

    - s:str, query
    - engine: (see patdb_tbox.psql.psql.create_engine) it can also be a connection

    Returns:

        - pd.DataFrame
    """
    if verbose >= 2:
        pidprint("\n", s, flag="info")
    start_dl_time = datetime.now()
    df = pd.read_sql(s, engine)
    end_dl_time = datetime.now()
    dl_time = (end_dl_time - start_dl_time).total_seconds()
    memusage_MB = df.memory_usage(index=True, deep=True).sum() / 1024 / 1024

    if verbose >= 1:
        pidprint("dl_time={} sec, volume={} MB, link speed={} MB/s".format(round(dl_time, 3), round(memusage_MB, 3),
                                                                           round(memusage_MB / dl_time, 3)),
                 flag="report")
    return df


def pidprint(*arg, flag="status"):
    """
    Behaves like builtin `print` function, but append runtime info before writing to stderr.
    The runtime info is:

    - pid
    - datetime.now()
    - flag, specified as a keyword
    """
    print("[{}] [{}] [{}]".format(os.getpid(), datetime.now(), flag), " ".join(map(str, arg)), file=sys.stderr)
    return


def decompress_string(sz: str, verbose=False):
    """
    Decompress strings encoded in `patdb_tbox.psql.psql.compress_string`

    Inputs

    - sz:str, utf8 character strings (see `patdb_tbox.psql.psql.compress_string`)
    - verbose: bool
    """

    if verbose:
        pidprint("Get base64 from zstring...")
    out = base64.b64decode(sz)

    if verbose:
        pidprint("Get decompressed bytes...")
    out = zlib.decompress(out)

    if verbose:
        pidprint("Decode string")
    return out.decode("utf8")


def decompress_chunk(sz: str, verbose=False, nrows=None):
    """
    **Obselete** decompress a dataframe.
    """
    s = decompress_string(sz, verbose=verbose)

    out = pd.DataFrame()
    if s != "\n":
        if verbose:
            pidprint("str2DataFrame...")

        out = pd.read_csv(StringIO(s), sep=";", nrows=nrows)

        if verbose:
            pidprint("Parse time columns...")
        for k in ["timestamp", "context__tl", "date"]:
            if k in out.columns:
                out[k] = pd.to_datetime(out[k])
    return out


def merge_sig(dd, k, date_col="timestamp"):
    """This aggregates the dataframes encoded in different columns of the ...__lf sql tables."""
    all_chunks = [decompress_chunk(s).drop(columns=["local_id"],
                                           errors="ignore").rename(columns={"date": "timestamp"})
                  for s in dd if not (s is None)
                  ]

    all_chunks = [c for c in all_chunks if c.shape[0] > 0]

    all_chunks = [c.rename(columns={l: k for l in [ll for ll in list(c) if ll != date_col]}).set_index(date_col)
                  for c in all_chunks]
    if len(all_chunks) > 0:
        out = pd.concat(all_chunks, axis=0, ignore_index=False, sort=True).sort_index()
    else:
        out = pd.DataFrame(index=pd.DatetimeIndex([], name="timestamp"), columns=[k])
    return out


def get_signals(d, signames=None, Ts="1S", date_col="timestamp"):
    """From the lf sql table, returns a dataframe with the data of similar signals aggregated and resampled."""

    allsigs = {
        k: merge_sig(clean_sig(d[v].dropna(axis=1, how='all')).values.reshape(-1), k,date_col=date_col).resample(Ts).apply(np.nanmean) for
        k, v in signames.items()
    }

    df = pd.concat(list(allsigs.values()), axis=1, sort=True).resample(Ts).apply(np.nanmean)
    return df


def set_lf_query(the_intervals):
    all_intervals = ", ".join(the_intervals)
    return "select * from monitorlf where ids__interval in ({})".format(all_intervals)


def get_tk_data(ids__uid, engine):
    with engine.connect() as con:
        dftk = pd.read_sql(
            "select * from takecare where ids__uid =\'{}\'".format(ids__uid),
            con)
    dftk.dropna(how='all', axis=1, inplace=True)
    pidprint("Takecare:", dftk.shape)

    all_evt = sum(dftk[[s for s in dftk.columns if s.startswith("tkevt__")]].values.tolist(), [])
    all_evt = [sum([ss.split("@") for ss in s.split("___")], []) for s in all_evt if not (s is None)]

    dftk = [[l[0], pd.to_datetime([l[1]])] for l in all_evt]
    pidprint("Takecare events:", len(dftk))
    return dftk


def signal_decomp(s, Ts=None, subsample=True):
    out = pd.DataFrame(columns=["date", "data"])
    s_uz = decompress_string(s)
    if len(s_uz) > 1:
        df = pd.read_csv(StringIO(s_uz), sep=';')
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S.000000%f")
        if not (Ts is None):
            df.set_index("date", inplace=True)
            df = df.resample(Ts).first().reset_index()
        df["data"] = list(map(partial(hfstr2df, subsample=subsample), df[["date", "data"]].values))
        out = pd.concat(df["data"].values, ignore_index=True)
    return out


def hfstr2df(l, subsample=None):
    ref_date, s = l
    out = pd.DataFrame()
    if not (s is None):
        a = np.array(list(map(float, decompress_string(s).split(";"))))
        ms = a[0]
        a = a[1:].reshape(-1, 1)
        dt = np.array([ref_date+pd.Timedelta(ms * i, "ms") for i in range(len(a))]).reshape(-1, 1)
        data = np.concatenate([dt, a], axis=1)
        out = pd.DataFrame(data=data, columns=["date", "data"])
        if not (subsample is None):
            out = out.set_index("date").resample(subsample).first().reset_index()
    return out


def get_hf_data(the_intervals, engine, Ts=None, subsample=pd.Timedelta(1,"s")):
    D = []
    with engine.connect() as con:
        for i_interv, theinterv in enumerate(the_intervals):
            pidprint("Interv:", i_interv + 1, "/", len(the_intervals))

            s_interv = "select * from monitorhf where ids__interval = {}".format(theinterv)

            dtmp = run_query(s_interv, con, verbose=1).dropna(axis=1)
            if dtmp.shape[1] > 0 and dtmp.shape[0] > 0:
                all_signames = {s: [s] for s in dtmp.columns if s.startswith("hf__")}
                dtmp = dtmp[list(all_signames.keys())].copy()
                dtmp = dtmp.applymap(partial(signal_decomp, Ts=Ts, subsample=subsample))

                for c, dd in zip(dtmp.columns, dtmp.values[0]):
                    dd.columns = ["date", c]

                if dtmp.shape[1] > 0 and dtmp.shape[0] > 0:
                    dtmp = pd.concat(dtmp.values[0], axis=0).set_index("date")
                    D.append(dtmp)

    dfmonhf = pd.concat(D, axis=0)
    dfmonhf[dfmonhf.columns] = dfmonhf[dfmonhf.columns].values.astype(np.float16)
    return dfmonhf

