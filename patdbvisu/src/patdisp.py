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

#def pat_data_q(tbl_name, ids__uid, col="*"):
#    return "select {} from {} where ids__uid=\'{}\'".format(col, tbl_name, ids__uid)
import os

import re
from datetime import datetime
import time

from functools import partial
import hashlib



# Remove empty
def clean_sig(d):
    return d[d != None]


def merge_sig(dd, k, date_col="timestamp"):
    """This aggregates the dataframes encoded in different columns of the ...__lf sql tables."""
    all_chunks = [decompress_chunk(s).drop(columns=["local_id"],errors="ignore").rename(columns={"date":"timestamp"}) for s in dd if not (s is None)]
    all_chunks = [c for c in all_chunks if c.shape[0] > 0]

    all_chunks = [c.rename(columns={l: k for l in [ll for ll in list(c) if ll != date_col]}).set_index(date_col)
                  for c in all_chunks]
    if len(all_chunks) > 0:
        out = pd.concat(all_chunks, axis=0, ignore_index=False, sort=True).sort_index()
    else:
        out = pd.DataFrame(index=pd.DatetimeIndex([],name="timestamp"), columns=[k])
    return out


def get_signals(d, signames=None, Te="1S",date_col="timestamp"):
    """From the lf sql table, returns a dataframe with the data of similar signals aggregated and resampled."""
    allsigs = {
        k: merge_sig(clean_sig(d[v].dropna(axis=1, how='all')).values.reshape(-1), k,date_col=date_col).resample(Te).apply(np.nanmean) for
        k, v in signames.items()}
    df = pd.concat(list(allsigs.values()), axis=1, sort=True).resample(Te).apply(np.nanmean)
    return df


def get_monitorlf_visual(ids__uid, engine, cache_root=".", data2=None, force_redraw=False):
    s_uid = "select ids__interval from view__monitorlf_has_onesignal vmha where ids__uid = '{}'".format(ids__uid)

    with engine.connect() as con:
        the_intervals = list(map(lambda ss: "'" + ss + "'", pd.read_sql(s_uid, con).values.reshape(-1).tolist()))
    all_intervals = ", ".join(the_intervals)
    s_interv = "select * from monitorlf where ids__interval in ({})".format(all_intervals)

    thehash_id = gethash(s_uid + s_interv)

    cache_fname = os.path.join(cache_root, thehash_id + "_monitorlf.pkl")

    if (not os.path.isfile(cache_fname)) or (force_redraw):
        with engine.connect() as con:
            dfmonitor = pd.read_sql(s_interv, con)

        pidprint("Downloaded:...", dfmonitor.shape)

        dfmon = get_signals(dfmonitor, signames=valid_signames, Te="10T")
        pidprint("Mondata:", dfmon.shape)

        with engine.connect() as con:
            dftk = pd.read_sql(
                "select * from takecare where ids__uid =\'{}\'".format(ids__uid),
                con)
        dftk.dropna(how='all', axis=1, inplace=True)
        pidprint("Takecare:", dftk.shape)


        all_evt = sum(dftk[[s for s in dftk.columns if s.startswith("tkevt__")]].values.tolist(),[])
        all_evt = [sum([ss.split("@") for ss in s.split("___")],[]) for s in all_evt if not (s is None)]


        dftk = [[l[0], pd.to_datetime([l[1]])]  for l in all_evt]

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
        sig_colors={"btb":"red","rf":"green","spo2":"blue"}
        for l in dftk:
            thecase = not (re.compile(event_d["sepsis"]).match(l[0]) is None)

            print(l[0], thecase)

            c = "darkred" if thecase else "black"
            thesize = 6 if thecase else 1

            the_plot_data += [go.Scatter(x=[l[1][0]]*10, y=np.linspace(0, 1, 10).tolist(),
                                         hovertemplate="{}<br>{}<br>{}".format(*l[0].replace("tkevt__", "").split("/")),
                                         mode='lines', line=dict(width=thesize, color=c), showlegend=False)]

        the_plot_data += [go.Scatter(x=dfmon.index,
                                   y=((dfmon[k] - dfmon.min().min()) / (dfmon.max().max() - dfmon.min().min()) * scale),
                                   name=k, line=dict(width=3, color=sig_colors[k])) for k in dfmon.columns]

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
              State("patdisp-input-patid", "value"))
def plot_patient(plot_button, patid):
    if plot_button is None:
        raise PreventUpdate

    print(patid.split(";"))

    if all(is_patid(p) for p in patid.split(";")):
        Figs = [get_monitorlf_visual(ids__uid, engine, cache_root="cache") for ids__uid in patid.split(";")]
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