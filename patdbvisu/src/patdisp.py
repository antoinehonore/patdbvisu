from src.utils import all_data_tables2,decompress_chunk,valid_signames,better_lookin, run_select_queries, gentbl_raw, all_data_tables, ref_cols, get_dbcfg, get_engine,get_update_status,pidprint

from startup import app,engine,all_cols
from src.events import d as event_d

import pandas as pd
import dash
from dash import html, Input, Output, State, dcc
from dash.exceptions import PreventUpdate
import numpy as np
import pickle as pkl
import plotly.express as px
import plotly.graph_objects as go
import os

import re
from datetime import datetime
import time

from functools import partial
import hashlib
from src.utils import get_colnames

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

all_the_monitorlf_cols = [s.strip("\"") for s in all_cols["monitorlf"] if s.strip("\"").startswith("lf__")]
indiv_sig_colors = {signame: colorname if len([grouped_sig_colors[k] for k,v in valid_signames.items() if signame in v])==0
                                        else [grouped_sig_colors[k] for k,v in valid_signames.items() if signame in v][0]

                    for signame, colorname in
                    zip(all_the_monitorlf_cols, ALL_COLORS[:len(all_the_monitorlf_cols)])}

all_the_monitorhf_cols = [s.strip("\"") for s in all_cols["monitorhf"] if s.strip("\"").startswith("hf__")]
indiv_hfsig_colors = {signame: colorname if len([grouped_sig_colors[k] for k,v in valid_signames.items() if signame in v])==0
                                        else [grouped_sig_colors[k] for k,v in valid_signames.items() if signame in v][0]

                    for signame, colorname in
                    zip(all_the_monitorhf_cols, ALL_COLORS[:len(all_the_monitorhf_cols)])}


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


def get_signals(d, signames=None, Te="1S", date_col="timestamp"):
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

def signal_decomp(s, Ts=None):

    out=pd.DataFrame(columns=["date","data"])
    s_uz = decompress_string(s)
    if len(s_uz)>1:
        df = pd.read_csv(StringIO(s_uz), sep=';')
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S.000000%f")
        if not (Ts is None):
            df.set_index("date", inplace=True)
            df = df.resample(Ts).first().reset_index()
        df["data"] = list(map(hfstr2df, df[["date", "data"]].values))
        out = pd.concat(df["data"].values, ignore_index=True)
    return out


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
        out = out.set_index("date").resample(pd.Timedelta(1, "s")).first().reset_index()
    return out


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


def get_hf_data(the_intervals, Ts=None):
    D = []
    for i_interv, theinterv in enumerate(the_intervals):
        pidprint("Interv:", i_interv + 1, "/", len(the_intervals))
        with engine.connect() as con:
            s_interv = "select * from monitorhf where ids__interval = {}".format(theinterv)

            dtmp = run_query(s_interv, con, verbose=True).dropna(axis=1)
            print(dtmp.shape)
            if dtmp.shape[1] > 0 and dtmp.shape[0] > 0:
                all_signames = {s: [s] for s in dtmp.columns if s.startswith("hf__")}
                dtmp = dtmp[list(all_signames.keys())].copy()
                dtmp = dtmp.applymap(partial(signal_decomp, Ts=Ts))
                for c, dd in zip(dtmp.columns, dtmp.values[0]):
                    dd.columns = ["date", c]
                print(dtmp.shape)
                if dtmp.shape[1] > 0 and dtmp.shape[0] > 0:
                    dtmp = pd.concat(dtmp.values[0], axis=0).set_index("date")
                    D.append(dtmp)

    dfmonhf = pd.concat(D, axis=0)
    dfmonhf[dfmonhf.columns] = dfmonhf[dfmonhf.columns].values.astype(np.float16)

    return dfmonhf#, {s: [s] for s in dfmonhf.columns}


def get_monitor_visual(ids__uid, engine, cache_root=".", data2=None, force_redraw=False, opts_signals=None):
    s_uid = "select ids__interval from view__monitorlf_has_onesignal vmha where ids__uid = '{}'".format(ids__uid)

    with engine.connect() as con:
        the_intervals = list(map(lambda ss: "'" + ss + "'", pd.read_sql(s_uid, con).values.reshape(-1).tolist()))

    all_intervals = ", ".join(the_intervals)
    s_interv = "select * from monitorlf where ids__interval in ({})".format(all_intervals)

    thehash_id = gethash(s_uid + s_interv + str(opts_signals))

    cache_fname = os.path.join(cache_root, thehash_id + "_monitor.pkl")
    Ts = "10T"

    #pidprint(opts_signals)
    pidprint(cache_fname)
    if (not os.path.isfile(cache_fname)) or (force_redraw):

        with engine.connect() as con:
            dfmonitor = pd.read_sql(s_interv, con)

        pidprint("Downloaded:...", dfmonitor.shape)
        disp_all_available = False
        get_hf=False

        if "available_lf" in opts_signals:
            disp_all_available=True
            all_signames = {k: [k] for k in dfmonitor.columns if k.startswith("lf__")}
            dfmon = get_signals(dfmonitor, signames=all_signames, Te=Ts)
            sig_colors = {k: indiv_sig_colors[k] for i, k in enumerate(all_signames.keys())}
        else:
            dfmon = get_signals(dfmonitor, signames=valid_signames, Te="10T")
            sig_colors = grouped_sig_colors

        if "waveform" in opts_signals:
            get_hf = True
            dfmonhf = get_hf_data(the_intervals, Ts=Ts)

            # Rescaling
            dfmonhf = (dfmonhf - dfmonhf.min()) / (dfmonhf.max() - dfmonhf.min()) / dfmonhf.shape[1] + 1 / \
                  dfmonhf.shape[1] * np.arange(dfmonhf.shape[1]).reshape(1, -1) +1

            pidprint("Mondata:", dfmonhf.shape,
                     ", ", round(dfmonhf.memory_usage(deep=True).sum()/1024/1024, 2), "MB")

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

            the_plot_data += [go.Scattergl(x=[l[1][0]]*10, y=np.linspace(0, 1, 10).tolist(),
                                         hovertemplate="{}<br>{}<br>{}".format(*l[0].replace("tkevt__", "").split("/")),
                                         mode='lines', line=dict(width=thesize, color=c), showlegend=False)]

        the_plot_data += [go.Scattergl(x=dfmon.index,
                                   y=((dfmon[k] - dfmon.min().min()) / (dfmon.max().max() - dfmon.min().min()) * scale),
                                    hovertemplate="<b>Date</b>: %{x}<br><b>Name</b>: "+k,
                                   name=k,
                                     showlegend= not disp_all_available,
                                     line=dict(width=3, color=sig_colors[k])) for k in dfmon.columns]
        if get_hf:
            the_plot_data += [go.Scattergl(   x=dfmonhf.index,
                                            y=dfmonhf[k],
                                            hovertemplate="<b>Date</b>: %{x}<br><b>Name</b>: "+k,
                                            mode='lines+markers',
                                            name=k,
                                            showlegend= not disp_all_available,
                                            marker=dict(color=indiv_hfsig_colors[k],),
                                            line=dict(width=3, color=indiv_hfsig_colors[k])) for k in dfmonhf.columns]

        fig = go.Figure(the_plot_data, dict(title="monitorLF for {}".format(ids__uid)))

        lgd += ["spo2", "btb", "rf"]

        with open(cache_fname, "wb") as fp:
            pkl.dump(fig, fp)

    else:
        with open(cache_fname, "rb") as fp:
            fig = pkl.load(fp)
    fig.update_layout(template="none")
    return fig



def read_salt(fname: str) -> str:
    """Read hash function salt from file."""
    with open(fname, "r", encoding="utf8") as f:
        out = f.read().strip()
    return out


def gethash(s: str, salt="") -> str:
    """Use the function returned by `patdb_tbox.pn.format_pn.init_hash_fun` instead."""
    if isinstance(s, str):
        out = hashlib.sha256((salt+s).encode("utf8")).hexdigest()
    else:
        out = hashlib.sha256((salt+str(s)).encode("utf8")).hexdigest()
    return out


def init_hash_fun(fname_salt="/opt/psql/pn_salt.txt") -> partial:
    """Returns the PN callable hash_function."""
    salt_str = read_salt(fname_salt)
    hash_fun = partial(gethash, salt=salt_str)
    return hash_fun


thisyear = str(datetime.now().year)


def format_pn(x) -> str:
    """
    Format personal numbers.

    All temporary numbers are formatted: 99yyyy-xxxxxx
    All the standard numbers are formatted: yyyymmdd-xxxx

    If the input does not match  re.compile("^[0-9]+-?[0-9]+$") (i.e. is only made of numbers potentially separated with a '-')
    then the output is returned.
    """

    # Can only remove the ambiguity if people are less than 100 years old
    if re_is_pn.fullmatch(x) is None: # is it not made of digits and potentially a '-'
        return x

    if x[:2] == '99':
        if '-' in x:
            s = x.split("-")
            if len(s[0]) == 6 and len(s[1]) == 6: #99yyyy-xxxxxx
                return x

            if len(s[0]) == 10 and len(s[1]) == 4: #99yyyymmdd-xxxx (faulty input)
                return x[2:]

        else:  #99yyyyxxxxxx
            return x[:6] + '-' + x[6:]
    else:
        if '-' in x:
            s = x.split("-")
            if len(s[0]) == 6 and len(s[1]) == 4:
                if int(s[0][:2]) <= int(thisyear[2:]):  #yymmdd-xxxx (supposely born with a year starting with 20. (i.e. we can t format PN of people born before 1921)
                    return '20' + x
                elif int(s[0][:2]) > int(thisyear[2:]):
                    return '19' + x
            if len(s[0]) == 4 and len(s[1]) == 6 and int(s[0]) <= int(thisyear):  #yyyy-xxxxxx
                return '99' + x
            if len(s[0]) == 8 and len(s[1]) == 4 and int(s[0]) <= int(thisyear):  #yyyymmdd-xxxx
                return x
        else:
            if len(x) == 12 and int(x[:4]) <= int(thisyear): #yyyymmddxxxx
                return x[:8] + '-' + x[8:]

            if len(x) == 10:
                if int(x[:2]) <= int(thisyear[2:]): #yymmddxxxx with yy <= 21
                    return '20' + x[:6] + '-' + x[6:]
                elif int(x[:2]) > int(thisyear[2:]): #yymmddxxxx with yy >21
                    return '19' + x[:6] + '-' + x[6:]
    return x


re_is_patid = re.compile("^[a-zA-Z0-9]*$")
re_is_pn = re.compile("^[0-9]+-?[0-9]+$")


def is_patid(s):
    return (len(str(s)) == 64) and (re_is_patid.fullmatch(str(s)))


def is_pn(s):
    return (len(str(s)) == 13) and (not (re_is_pn.fullmatch(str(s))is None))


def prep_token(s):
    f = init_hash_fun()
    thehash = f(s)
    return thehash


def search_id(token, cfg_root="cfg"):
    dbcfg = get_dbcfg(os.path.join(cfg_root, "pnuid.cfg"))
    engine = get_engine(verbose=False, **dbcfg)
    with engine.connect() as con:
        d = pd.read_sql("select * from themap where ids__uid='{}'".format(token), con)

    return d


@app.callback(
    Output("patdisp-latestupdate", "children"),
    Output("patdisp-convert-disp", "children"),
    Output("patdisp-interv-dropdown", "options"),
    Output("patdisp-interv-dropdown", "value"),
    Input("patdisp-search-button", "n_clicks"),
    Input("patdisp-convert-button", "n_clicks"),
    Input("patdisp-input-patid", "value")
)
def cb_render(n_clicks, n_click_cv, patid):
    empty_out = [[], []]
    if (n_clicks is None) and (n_click_cv is None):
        raise PreventUpdate
    else:
        ctx = dash.callback_context

        if not ctx.triggered:
            raise PreventUpdate
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if button_id == "patdisp-input-patid":
            raise PreventUpdate

        start_ = datetime.now()
        out = [html.P("Wrong input format (neither a valid PN nor a valid ids__uid): {}".format(patid)), [], []]
        if is_patid(patid):
            if button_id == "patdisp-search-button":
                with engine.connect() as con:
                    id_in_db = pd.read_sql("select * from view__uid_all where ids__uid = '{}'".format(patid), con)

                if id_in_db.shape[0] == 0:
                    out = [html.P("No data found in DB for ids__uid: {}".format(patid))] + empty_out

                else:
                    create_views_queries = {
                        k: "create or replace view patview_{}_{} as (select * from {} where ids__uid='{}');".format(k,
                                                                                                                    patid,
                                                                                                                    k,
                                                                                                                    patid)
                        for k
                        in all_data_tables}

                    engine.execute("\n".join([v for v in create_views_queries.values()]))

                    select_queries = {k: "select {} from patview_{}_{}".format(the_cases[k], k, patid) for k in
                                      all_data_tables}

                    data_lvl1 = run_select_queries(select_queries, engine)

                    data_lvl1_l = [html.P("{} {}".format(k, str(v.shape))) for k, v in data_lvl1.items() if
                                   k != "overview"]

                    all_interv = {k: v[["ids__interval", "interval__start", "interval__end"]] for k, v in
                                  data_lvl1.items() if
                                  all([c in v.columns for c in ["ids__interval", "interval__start", "interval__end"]])}
                    if any([v.shape[0] > 0 for v in all_interv.values()]):
                        all_interv = pd.concat([v for v in all_interv.values()], axis=0)

                    all_interv = all_interv.drop_duplicates().sort_values(by="interval__start")

                    options = [{"label": "All", "value": "all"}] \
                              + [{"label": "{} -> {}".format(s, e), "value": "'" + id + "'"}
                                 for id, s, e in all_interv.values]

                    value = []
                    out = [None, options, value]

            elif button_id == "patdisp-convert-button":
                time.sleep(1)
                answer = search_id(str(patid))
                out = [html.P("{} | {}".format(*tuple(answer.values.reshape(-1).tolist()))), [], []]

        elif is_pn(str(patid)):

            if button_id == "patdisp-convert-button":
                time.sleep(1)
                answer = search_id(prep_token(str(patid)))

                out = [html.P("{} | {}".format(*tuple(answer.values.reshape(-1).tolist()))), [], []]
            else:
                out = [html.P("PN not found in DB: {}".format(patid)), [], []]

        return [get_update_status(start_)] + out


@app.callback(Output("patdisp-plot-disp", "children"),
              Input("patdisp-plot-button", "n_clicks"),
              State("patdisp-input-patid", "value"),
              State("patdisp-plot-checklist", "value"))
def plot_patient(plot_button, patid_, opts_signals):
    if plot_button is None:
        raise PreventUpdate

    patid = patid_.strip(";")
    print(patid.split(";"))

    if all(is_patid(p) for p in patid.split(";")):
        Figs = [get_monitor_visual(ids__uid, engine, cache_root="cache", opts_signals=opts_signals) for ids__uid in patid.split(";")]
        return sum([[html.P(ids__uid), dcc.Graph(figure=fig, style={"margin-top": "50px"})] for ids__uid,fig in zip(patid.split(";"), Figs)],[])
    else:
        return None

# 71f3f1ab2f42253f2fe36886720ef5353a2fe84244dd19c031dc4f4b1a189700
thecase = "case when ({} notnull) then True else NULL end as {}"


the_cases = {k: ",\n".join([col if (k == "overview") or (col in ref_cols) else thecase.format(col, col)
                            for col in all_cols[k]]) for k in all_data_tables}


@app.callback(
    Output("patdisp-figures", "children"),
    Input("patdisp-interv-dropdown", "value"),
    Input("patdisp-interv-dropdown", "options"),
    Input("patdisp-input-patid", "value"),
    Input("patdisp-display-button", "n_clicks")
)
def display_patient_interv(val, opts, patid, n_clicks):
    ctx = dash.callback_context
    if n_clicks is None:
        raise PreventUpdate

    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    print(button_id)

    out = []
    if button_id == "patdisp-display-button":
        if len(val) > 0:
            if "all" in val:
                interv_l = [v["value"] for v in opts[1:]]
            else:
                interv_l = val

            select_queries = {k: "select {} from patview_{}_{} where ids__interval in ({})".format(the_cases[k],
                                                                                                   k,
                                                                                                   patid,
                                                                                                   ",".join(interv_l))
                              for k in all_data_tables2
                              }

            data_lvl1 = run_select_queries(select_queries, engine)
            data_lvl1 = {k: v.sort_values(by="interval__start") if "interval__start" in v.columns else v for k, v in
                         data_lvl1.items()}

            data_lvl1 = {k: v[[c for c in v.columns if not (c in ["ids__uid", "ids__interval", "interval__raw"])]] for
                         k, v in data_lvl1.items()}

            df = pd.concat([v.set_index(["interval__start", "interval__end"]) for v in data_lvl1.values() if all([s in v.columns for s in ["interval__start", "interval__end"]])], axis=1)

            out = [html.Div(
                [html.P(k), gentbl_raw(v, id="tbl_{}".format(k), style_table={'overflowX': 'auto'}), html.Br()]) for
                k, v in data_lvl1.items()]
            out += [html.Div([html.P("full table"),
                              gentbl_raw(df.reset_index(),
                                         id="patientid-fulltbl",
                                         style_table={'overflowX': 'auto'})
                              ]
                             )
                    ]
    return out