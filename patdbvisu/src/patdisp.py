from src.utils import all_data_tables2, \
    valid_signames, better_lookin, run_select_queries, gentbl_raw,\
    all_data_tables, ref_cols, get_update_status, \
    pidprint, get_colnames

from startup import app, engine, all_cols
from src.events import d as event_d
from utils_db.utils_db import get_hf_data, set_lf_query, get_signals,\
    get_tk_data,run_query,get_engine,get_dbcfg, get_resp_data
import pandas as pd
import dash
from dash import html, Input, Output, State, dcc
from dash.exceptions import PreventUpdate
import numpy as np
import pickle as pkl
import plotly.graph_objects as go
import os

import re
from datetime import datetime
import time

from functools import partial
import hashlib

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

resp_colors = {
    "respirator_cpap": "darkgreen",
    "respirator_fabian":"black",
    "respirator_hfo":"red",
    "respirator_nasal__cpap": "darkgreen",
    "respirator_sipap__biphasic__tr": "blue",
    "respirator_sipap__biphasic__trapn": "blue",
    "respirator_sipap__biphasic_duop": "blue",
    "respirator_sippv": "orange",
    "respirator_standby": "black",
    "respirator_unknown": "black",
    "respirator_o2__therapy": "green",
    "respirator_bilevel":"orange",
    "respirator_advance__cpap_apne": "darkgreen",
    "respirator_tk": "orange",
    "respirator_advance__cpap__trpa":"darkgreen",
    "respirator_pc_ps_auto_no__pat__tr":"orange",
    "respirator_pc_ps_auto_pat__trigg":"orange",
    "respirator_ps_cpap": "darkgreen",
    "respirator_ippv_imv": "orange",
    "respirator_simv_neonatology_":"orange",
    "respirator_tu_cpap": "darkgreen",
    "respirator_ncpap": "darkgreen",
    "respirator_hogt__flode": "darkgreen",
    "respirator_inget": "black",
    "respirator_nasal__cpapippv_imv": "darkgreen",
    "respirator_21": "black",
    "respirator_nasal_cpap": "darkgreen",
    "respirator_vkts": "orange",
    "respirator_bilevelsippv": "orange"
}


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


def get_lf_data(the_intervals, engine, Ts="10T", disp_all_available=False):
    s_interv = set_lf_query(the_intervals)

    dfmonitor = run_query(s_interv, engine, verbose=1)

    if disp_all_available:
        all_signames = {k: [k] for k in dfmonitor.columns if k.startswith("lf__")}
        dfmon = get_signals(dfmonitor, signames=all_signames, Ts=Ts)
        sig_colors = {k: indiv_sig_colors[k] for i, k in enumerate(all_signames.keys())}
    else:
        dfmon = get_signals(dfmonitor, signames=valid_signames, Ts=Ts)
        sig_colors = grouped_sig_colors
    return dfmon, sig_colors


def get_monitor_visual(ids__uid, engine, cache_root=".", data2=None,
                       force_redraw=False, opts_signals=None, verbose=2):

    s_uid = "select ids__interval from view__interv_all where ids__uid = '{}'".format(ids__uid)

    with engine.connect() as con:
        the_intervals = list(map(lambda ss: "'" + ss + "'", pd.read_sql(s_uid, con).values.reshape(-1).tolist()))

    if len(the_intervals) == 0:
        return None

    s_interv = set_lf_query(the_intervals)

    thehash_id = gethash(s_uid + str(opts_signals))

    cache_fname = os.path.join(cache_root, thehash_id + "_monitor.pkl")

    Ts = "10T"
    subsample = pd.Timedelta(2, "s")

    pidprint(cache_fname)

    if (not os.path.isfile(cache_fname)) or (force_redraw):
        pidprint("File was not found in cache ...")

        disp_all_available = "available_lf" in opts_signals

        resp_plot_data = []

        dfmon, sig_colors = get_lf_data(the_intervals, engine, Ts=Ts, disp_all_available=disp_all_available)

        get_hf = "waveform" in opts_signals
        get_resp = "respirator" in opts_signals

        if get_resp:
            dfresp = get_resp_data(ids__uid, engine, verbose=verbose)

            for iresp, k in enumerate(dfresp.columns):
                resp_plot_data.append(go.Scattergl(x=dfresp.index,
                                                   y=dfresp[k]-1.5,
                                                   hovertemplate="<b>Date</b>: %{x}<br><b>Name</b>: " + k.replace("respirator_",""),
                                                   mode='markers',
                                                   name=k.replace("respirator_", ""),
                                                   showlegend=True,
                                                   marker=dict(color=resp_colors[k])
                                                   )
                                      )

        if get_hf:
            dfmonhf = get_hf_data(the_intervals, engine, Ts=Ts, subsample=subsample, verbose=verbose)
            #pidprint("finished")
            if verbose >= 2:
                pidprint("dfmonhf.shape", dfmonhf.shape)
                #pidprint("Range:",(dfmonhf.max() - dfmonhf.min()))
            if dfmonhf.shape[0]>0:
                # Rescaling
                dfmonhf = (dfmonhf - dfmonhf.min()) / (dfmonhf.max() - dfmonhf.min()) / dfmonhf.shape[1] + 1 / \
                      dfmonhf.shape[1] * np.arange(dfmonhf.shape[1]).reshape(1, -1) + 1

            pidprint("Mondata:", dfmonhf.shape,
                     ", ", round(dfmonhf.memory_usage(deep=True).sum()/1024/1024, 2), "MB")

        dftk = get_tk_data(ids__uid, engine)

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
            thecase_sepsis = not (re.compile(event_d["sepsis"]).match(l[0]) is None)

            print(l[0], thecase_sepsis)

            c = "darkred" if thecase_sepsis else "black"
            thesize = 6 if thecase_sepsis else 1

            the_plot_data += [go.Scattergl(x=[l[1][0]]*10, y=np.linspace(0, 1, 10).tolist(),
                                         hovertemplate="{}<br>{}<br>{}".format(*l[0].replace("tkevt__", "").split("/")),
                                         mode='lines', line=dict(width=thesize, color=c), showlegend=False)]

        the_plot_data += [go.Scattergl( x=dfmon.index,
                                        y=((dfmon[k] - dfmon.min().min()) / (dfmon.max().max() - dfmon.min().min()) * scale),
                                        hovertemplate="<b>Date</b>: %{x}<br><b>Name</b>: "+k,
                                        name=k,
                                        showlegend=not disp_all_available,
                                        line=dict(width=3, color=sig_colors[k])) for k in dfmon.columns]

        if get_hf:
            the_plot_data += [go.Scattergl(x=dfmonhf.index,
                                           y=dfmonhf[k],
                                           hovertemplate="<b>Date</b>: %{x}<br><b>Name</b>: "+k,
                                           mode='markers',
                                           name=k,
                                           showlegend=not disp_all_available,
                                           marker=dict(color=indiv_hfsig_colors[k],),
                                           line=dict(width=3, color=indiv_hfsig_colors[k])) for k in dfmonhf.columns]

        the_plot_data += resp_plot_data
        if verbose >= 1:
            pidprint("Generate plot...")
        fig = go.Figure(the_plot_data, dict(title="monitorLF for {}".format(ids__uid)))

        lgd += ["spo2", "btb", "rf"]
        if verbose >= 1:
            pidprint("Save plot...")
        with open(cache_fname, "wb") as fp:
            pkl.dump(fig, fp)

    else:
        if verbose >= 1:
            pidprint("File was found in cache ...")
        with open(cache_fname, "rb") as fp:
            fig = pkl.load(fp)
    fig.update_layout(template="none")

    if verbose >= 1:
        pidprint("Done.")

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
    Input('patdisp-clear-button', "n_clicks"),
    Input("patdisp-input-patid", "value")
)
def cb_render(n_clicks, n_click_cv, n_clicks_clear, patid):
    empty_out = [[], []]
    if (n_clicks is None) and (n_click_cv is None) and (n_clicks_clear is None):
        raise PreventUpdate
    else:
        ctx = dash.callback_context

        if not ctx.triggered:
            raise PreventUpdate
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if button_id == "patdisp-input-patid":
            raise PreventUpdate

        if button_id == "patdisp-clear-button":
            return [None, html.P(""), [], []]

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

    thesep = " " if not (";" in patid_) else ";"

    patid = patid_.strip(thesep)
    print(patid.split(thesep))

    if all(is_patid(p) for p in patid.split(thesep)):
        Figs = [get_monitor_visual(ids__uid, engine, cache_root="cache", opts_signals=opts_signals) for ids__uid in patid.split(thesep)]
        return sum([[html.P(ids__uid), make_fig(fig)] for ids__uid, fig in zip(patid.split(thesep), Figs)], [])
    else:
        return None


def make_fig(fig):
    if fig is None:
        out = html.P("[error] No data were found.")
    else:
        out = dcc.Graph(figure=fig, style={"margin-top": "50px"})
    return out


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