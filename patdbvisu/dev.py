from src.utils import all_data_tables2, decompress_chunk, valid_signames,\
    better_lookin, run_select_queries, gentbl_raw, all_data_tables, \
    ref_cols, get_dbcfg, get_engine, get_update_status, pidprint,get_colnames
import os
import pandas as pd
import hashlib
import plotly.graph_objects as go
import numpy as np
import re
import pickle as pkl
from src.events import d as event_d

dbcfg = get_dbcfg("cfg/db.cfg")
engine = get_engine(verbose=False, **dbcfg)

# The tables column names
with engine.connect() as con:
    all_cols = {k: get_colnames(k, con) for k in all_data_tables}

ALL_COLORS = ["aliceblue", "antiquewhite", "aqua", "aquamarine", "azure",
                "beige", "bisque", "black", "blanchedalmond",
                "blueviolet", "brown", "burlywood", "cadetblue",
                "chartreuse", "chocolate", "coral", "cornflowerblue",
                "cornsilk", "crimson", "cyan", "darkblue", "darkcyan",
                "darkgoldenrod", "darkgray", "darkgrey", "darkgreen",
                "darkkhaki", "darkmagenta", "darkolivegreen", "darkorange",
                "darkorchid", "darkred", "darksalmon", "darkseagreen",
                "darkslateblue", "darkslategray", "darkslategrey",
                "darkturquoise", "darkviolet", "deeppink", "deepskyblue",
                "dimgray", "dimgrey", "dodgerblue", "firebrick",
                "floralwhite", "forestgreen", "fuchsia", "gainsboro",
                "ghostwhite", "gold", "goldenrod", "gray", "grey",
                "greenyellow", "honeydew", "hotpink", "indianred", "indigo",
                "ivory", "khaki", "lavender", "lavenderblush", "lawngreen",
                "lemonchiffon", "lightblue", "lightcoral", "lightcyan",
                "lightgoldenrodyellow", "lightgray", "lightgrey",
                "lightgreen", "lightpink", "lightsalmon", "lightseagreen",
                "lightskyblue", "lightslategray", "lightslategrey",
                "lightsteelblue", "lightyellow", "lime", "limegreen",
                "linen", "magenta", "maroon", "mediumaquamarine",
                "mediumblue", "mediumorchid", "mediumpurple",
                "mediumseagreen", "mediumslateblue", "mediumspringgreen",
                "mediumturquoise", "mediumvioletred", "midnightblue",
                "mintcream", "mistyrose", "moccasin", "navajowhite", "navy",
                "oldlace", "olive", "olivedrab", "orange", "orangered",
                "orchid", "palegoldenrod", "palegreen", "paleturquoise",
                "palevioletred", "papayawhip", "peachpuff", "peru", "pink",
                "plum", "powderblue", "purple", "rosybrown",
                "royalblue", "saddlebrown", "salmon", "sandybrown",
                "seagreen", "seashell", "sienna", "silver", "skyblue",
                "slateblue", "slategray", "slategrey", "springgreen",
                "steelblue", "tan", "teal", "thistle", "tomato", "turquoise",
                "violet", "wheat", "yellow",
                "yellowgreen"
            ] * 5

grouped_sig_colors = {"btb": "red", "rf": "green", "spo2": "blue"}

all_the_monitorhf_cols = [s.strip("\"") for s in all_cols["monitorhf"] if s.strip("\"").startswith("hf__")]
indiv_hfsig_colors = {signame: colorname if len([grouped_sig_colors[k] for k,v in valid_signames.items() if signame in v])==0
                                        else [grouped_sig_colors[k] for k,v in valid_signames.items() if signame in v][0]

                    for signame, colorname in
                    zip(all_the_monitorhf_cols, ALL_COLORS[:len(all_the_monitorhf_cols)])}


def gethash(s: str, salt="") -> str:
    """Use the function returned by `patdb_tbox.pn.format_pn.init_hash_fun` instead."""
    if isinstance(s, str):
        out = hashlib.sha256((salt+s).encode("utf8")).hexdigest()
    else:
        out = hashlib.sha256((salt+str(s)).encode("utf8")).hexdigest()
    return out

# Remove empty
def clean_sig(d):
    return d[d != None]


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


def get_hf_signals(d, signames=None, Te="1S", date_col="timestamp"):
    """From the lf sql table, returns a dataframe with the data of similar signals aggregated and resampled."""

    allsigs = {
        k: merge_sig(clean_sig(d[v].dropna(axis=1, how='all')).values.reshape(-1), k,date_col=date_col).resample(Te).apply(np.nanmean) for
        k, v in signames.items()
    }

    df = pd.concat(list(allsigs.values()), axis=1, sort=True).resample(Te).apply(np.nanmean)
    return df
import zlib
import base64
from io import StringIO

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

from functools import partial

from datetime import datetime


def run_query(s: str, engine, verbose=False) -> pd.DataFrame:
    """
    Runs the query specified as a string vs a db engine.

    Inputs:

    - s:str, query
    - engine: (see patdb_tbox.psql.psql.create_engine) it can also be a connection

    Returns:

        - pd.DataFrame
    """
    if verbose:
        pidprint("\n", s, flag="info")
    start_dl_time = datetime.now()
    df = pd.read_sql(s, engine)
    end_dl_time = datetime.now()
    dl_time = (end_dl_time - start_dl_time).total_seconds()
    memusage_MB = df.memory_usage(index=True, deep=True).sum() / 1024 / 1024

    if verbose:
        pidprint("dl_time={} sec, volume={} MB, link speed={} MB/s".format(round(dl_time, 3), round(memusage_MB, 3),
                                                                           round(memusage_MB / dl_time, 3)),
                 flag="report")
    return df


def signal_decomp(s, Ts=None):
    df = pd.read_csv(StringIO(decompress_string(s)), sep=';')
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S.000000%f")
    if not (Ts is None):
        df.set_index("date", inplace=True)
        df = df.resample(Ts).first().reset_index()
    df["data"] = list(map(hfstr2df, df[["date", "data"]].values))
    return pd.concat(df["data"].values, ignore_index=True)


def hfstr2df(l):
    ref_date, s = l
    out = pd.DataFrame()
    if not (s is None):
        a = np.array(list(map(float, decompress_string(s).split(";"))))
        ms = a[0]
        a = a[1:].reshape(-1, 1)
        dt = np.array([ref_date+pd.Timedelta(ms * i, "ms") for i in range(len(a))]).reshape(-1, 1)
        data = np.concatenate([dt, a], axis=1)
        out = pd.DataFrame(data=data, columns=["date", "data"])
    return out


def get_hf_data(the_intervals,Ts=None):
    D = []
    for theinterv in the_intervals:
        with engine.connect() as con:
            s_interv = "select * from monitorhf where ids__interval = {}".format(theinterv)

            dtmp = run_query(s_interv, con, verbose=True).dropna(axis=1)
            all_signames = {s: [s] for s in dtmp.columns if s.startswith("hf__")}

            dtmp = dtmp[list(all_signames.keys())].copy()
            dtmp = dtmp.applymap(partial(signal_decomp, Ts=Ts))
            for c, dd in zip(dtmp.columns, dtmp.values[0]):
                dd.columns = ["date", c]
                # dd[c] = dd[c].values.astype(np.float)
            dtmp = pd.concat(dtmp.values[0], axis=0).set_index("date")
            # dtmp = dtmp.resample(Ts).mean()

            D.append(dtmp)

    dfmonhf = pd.concat(D, axis=0)
    dfmonhf[dfmonhf.columns] = dfmonhf[dfmonhf.columns].values.astype(float)
    return dfmonhf,all_signames


def get_monitorhf_visual(ids__uid, engine, cache_root=".", data2=None, force_redraw=False, opts_signals=None):
    s_uid = "select ids__interval from monitorhf where ids__uid = '{}'".format(ids__uid)

    with engine.connect() as con:
        the_intervals = list(map(lambda ss: "'" + ss + "'", pd.read_sql(s_uid, con).values.reshape(-1).tolist()))

    all_intervals = ", ".join(the_intervals)
    s_interv = "select * from monitorhf where ids__interval in ({})".format(all_intervals)

    thehash_id = gethash(s_uid + s_interv + str(opts_signals))

    cache_fname = os.path.join(cache_root, thehash_id + "_monitorhf.pkl")

    Ts = "10T"

    if (not os.path.isfile(cache_fname)) or force_redraw:

        dfmonhf, all_signames = get_hf_data(the_intervals, Ts=Ts)

        # Rescaling
        dfmonhf = (dfmonhf-dfmonhf.min())/(dfmonhf.max()-dfmonhf.min())/dfmonhf.shape[1] + 1/dfmonhf.shape[1]*np.arange(dfmonhf.shape[1]).reshape(1,-1)

        sig_colors = {k: indiv_hfsig_colors[k] for i, k in enumerate(all_signames.keys())}

        pidprint("Mondata:", dfmonhf.shape,
                 ", ", round(dfmonhf.memory_usage(deep=True).sum()/1024/1024, 2), "MB")

        disp_all_available = dfmonhf.shape[1] > 0
        # The take care data
        with engine.connect() as con:
            dftk = pd.read_sql(
                "select * from takecare where ids__uid =\'{}\'".format(ids__uid), con)
        dftk.dropna(how='all', axis=1, inplace=True)
        pidprint("Takecare:", dftk.shape)

        all_evt = sum(dftk[[s for s in dftk.columns if s.startswith("tkevt__")]].values.tolist(), [])
        all_evt = [sum([ss.split("@") for ss in s.split("___")], []) for s in all_evt if not (s is None)]

        dftk = [[l[0], pd.to_datetime([l[1]])] for l in all_evt]

        pidprint("Takecare events:", len(dftk))
        pidprint("Plot...")

        lgd = []
        the_plot_data = []

        if not (data2 is None):
            the_plot_data += [go.Scatter(x=data2["timeline"],
                                         y=data2['dose'],
                                         name="dose")]
            the_plot_data += [go.Scatter(x=data2["timeline"],
                                         y=data2['weight'],
                                         name="dose")]

            scale = data2.set_index("timeline")[['dose', "weight"]].max().max()
            lgd += ['dose', "weight"]
        else:
            scale = 1

        # f19d8d014f398a43679b44ec736b4adeda36945890c3444e66a6f4d9afe7de7c

        for l in dftk:
            thecase = not (re.compile(event_d["sepsis"]).match(l[0]) is None)

            print(l[0], thecase)

            c = "darkred" if thecase else "black"
            thesize = 6 if thecase else 1

            the_plot_data += [go.Scatter(x=[l[1][0]]*10, y=np.linspace(0, 1, 10).tolist(),
                                         hovertemplate="{}<br>{}<br>{}".format(*l[0].replace("tkevt__", "").split("/")),
                                         mode='lines', line=dict(width=thesize, color=c), showlegend=False)]

        the_plot_data += [go.Scatter(   x=dfmonhf.index,
                                        y=((dfmonhf[k] - dfmonhf.min().min()) / (dfmonhf.max().max() - dfmonhf.min().min()) * scale),
                                        hovertemplate="<b>Date</b>: %{x}<br><b>Name</b>: "+k,
                                        mode='markers',
                                        name=k,
                                        showlegend= not disp_all_available,
                                        line=dict(width=3, color=sig_colors[k])) for k in dfmonhf.columns]

        fig = go.Figure(the_plot_data, dict(title="monitor for {}".format(ids__uid)))

        lgd += ["spo2", "btb", "rf"]

        with open(cache_fname, "wb") as fp:
            pkl.dump(fig, fp)

    else:
        with open(cache_fname, "rb") as fp:
            fig = pkl.load(fp)
    fig.update_layout(template="none")
    return fig


if __name__ == "__main__":
    ids__uid = "a120b4393ca0162f92cae3619c507934c9f19a44dca56f8dfe3f5559e1c2d4dc"
    fig = get_monitorhf_visual(ids__uid, engine, cache_root=".", data2=None, force_redraw=True, opts_signals=None)
    print("")
    pass
