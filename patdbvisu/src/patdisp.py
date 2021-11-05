from src.utils import all_data_tables2, run_select_queries, gentbl_raw, all_data_tables, ref_cols, get_dbcfg, get_engine,get_update_status

from startup import app,engine,all_cols

import pandas as pd
import dash
from dash import html, Input, Output
from dash.exceptions import PreventUpdate


#def pat_data_q(tbl_name, ids__uid, col="*"):
#    return "select {} from {} where ids__uid=\'{}\'".format(col, tbl_name, ids__uid)
import os

import re
from datetime import datetime
import time

from functools import partial
import hashlib

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


re_is_patid = re.compile("^[a-zA-Z0-9]*$")
re_is_pn = re.compile("^[0-9]+-?[0-9]+$")


def is_patid(s):
    return (len(str(s)) == 64) and (re_is_patid.fullmatch(str(s)))


def is_pn(s):
    return (len(str(s)) == 13) and (re_is_pn.fullmatch(str(s)))

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
    empty_out = [None, [], []]
    if (n_clicks is None) and (n_click_cv is None):
        raise PreventUpdate
    else:
        ctx = dash.callback_context

        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if button_id == "input-patid":
            raise PreventUpdate

        start_ = datetime.now()
        out = [html.P("Unknown input: {}".format(patid)), [], []]

        if is_patid(patid):
            if button_id == "patsearch-button":
                with engine.connect() as con:
                    id_in_db = pd.read_sql("select * from view__uid_all where ids__uid = '{}'".format(patid), con)

                if id_in_db.shape[0] == 0:
                    out = [html.P("ids__uid not found in DB: {}".format(patid))] + empty_out

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
                    all_interv = pd.concat([v for v in all_interv.values()], axis=0)

                    all_interv = all_interv.drop_duplicates().sort_values(by="interval__start")

                    options = [{"label": "All", "value": "all"}] \
                              + [{"label": "{} -> {}".format(s, e), "value": "'" + id + "'"}
                                 for id, s, e in all_interv.values]

                    value = []
                    out = [None, options, value]

            elif button_id == "patconvert-button":
                time.sleep(1)
                answer = search_id(str(patid))
                out = [gentbl_raw(answer, id="convert-res-tbl", style_table={"width": "450px"}), [], []]

        elif is_pn(str(patid)):

            if button_id == "patconvert-button":
                time.sleep(1)
                answer = search_id(prep_token(str(patid)))
                out = [gentbl_raw(answer, id="convert-res-tbl", style_table={"width": "450px"}), [], []]
            else:
                out = [html.P("PN not found in DB: {}".format(patid)), [], []]

        return [get_update_status(start_)] + out


thecase = "case when ({} notnull) then True else NULL end as {}"

the_cases = {k: ",\n".join([col if (k == "overview") or (col in ref_cols) else thecase.format(col, col)
                            for col in all_cols[k]]) for k in all_data_tables}


@app.callback(
    Output("patdisp-figures", "children"),
    Input("patdisp-interv-dropdown", "value"),
    Input("patdisp-interv-dropdown", "options"),
    Input("patdisp-input-patid", "value"),
    Input("patdisp-plot-button", "n_clicks")
)
def plot_patient_interv(val, opts, patid, n_clicks):
    ctx = dash.callback_context
    if n_clicks is None:
        raise PreventUpdate

    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    print(button_id)
    out = []
    if button_id == "patientid-disp-plot-button":
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
            out += [html.Div([html.P("full table"), gentbl_raw(df.reset_index(),
                                                               id="patientid-fulltbl",
                                                               style_table={'overflowX': 'auto'})
                              ]
                             )
                    ]
    return out