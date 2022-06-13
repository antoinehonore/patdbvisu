from datetime import datetime
import os
import sys
import pandas as pd
import json
from dash import dcc, html
from dash import dash_table as dt

# Overwrting some CSS default styles
style_tbl = dict(
    filter_action="native",
    sort_action="native",
    sort_mode="multi",
    style_data={'color': 'black', 'backgroundColor': 'white'},
    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(220, 220, 220)', }],
    style_header={'backgroundColor': 'rgb(210, 210, 210)', 'color': 'black', 'fontWeight': 'bold'}
)

thestyle = {"margin": "auto"}
nav_bar_style = {}

date_fmt = "%Y-%m-%d %H:%M:%S"

valid_signames = {"spo2": ["lf__150456__150456__150456__spo-__spo-__perc", "lf__none__150456__none__spo2__none__perc",
                     "lf__150456__150456__150472__spo-__spo-__perc", "lf__150456__150456__150476__spo-__spo-__perc",
                     "lf__150456__150456__192960__spo-__spo-__perc", "lf__150456__150456__192980__spo-__spo-__perc",
                     "lf__none__150456__none__spo2h__none__perc", "lf__150456__150456__150456__spo-__spo-__ok-nd"],
            "btb": ["lf__147840__147850__147850__btbhf__btbhf__spm", "lf__none__147850__none__btbhf__none__spm"],
            "rf": ["lf__151562__151562__151562__rf__rf__rpm", "lf__none__151562__none__rf__none__rpm",
                   "lf__151562__151562__151562__rf__rf__ok-nd", "lf__none__151562__none__rf__none__okand"]
            }


def better_lookin(ax, fontsize=20, lightmode=False):
    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                 ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(fontsize)
    ax.legend(fontsize=fontsize)
    ax.grid("on")
    if lightmode:
        ax.axis("off")
    else:
        ax.spines['right'].set_visible(False)
        # ax.spines['left'].set_visible(False)
        ax.spines['top'].set_visible(False)

def gdate(date_fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.now().strftime(date_fmt)


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



def get_dbcfg(fname):
    with open(fname, "rb") as fp:
        dbcfg = json.load(fp)
    return dbcfg


def gentbl(df, style_table=None):
    return dt.DataTable(id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **style_tbl, page_size=10, style_table=style_table)


def gentbl_raw(df, id="newtbl", **kwargs):
    return dt.DataTable(id=id,
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **kwargs)


def create_completion_dropdown():
    all_labels = ["Takecare", "Clinisoft", "Monitor LF", "Monitor HF"]
    all_values = [k.lower().replace(" ", "") for k in all_labels]
    tmp = dcc.Dropdown(
        options=[{"label": l, "value": v} for l, v in zip(all_labels, all_values)],
        value=all_values,
        # labelStyle={"display": "inline-block"},
        id="completion-dropdown",
        multi=True,
        placeholder="Choose the data sources",
        style=dict(width="450px")
    )
    return tmp


def get_colnames(k, con):
    df = pd.read_sql("select column_name   FROM information_schema.columns "\
                   " WHERE table_schema = \'public\'    AND table_name   = \'{}\' ".format(k),
                   con)
    return list(map(lambda s: "\""+s+"\"", df["column_name"].values.tolist()))


def get_latest_update(id, **kwargs):
    return html.Div([html.H3("Latest update: "), html.Div([html.P("-"), html.P("-")], id=id)], **kwargs)


def register_ids(themap, con):

    for k, v in themap.items():
        d = pd.read_sql("select ids__uid from themap where ids__uid='{}'".format(k), con)
        if d.empty:
            con.execute("insert into themap values ('{}','{}');".format(k, v))
            pidprint(k, "inserted.", flag="report")
        else:
            pidprint(k, "already found.", flag="report")


def read_query_file(fname: str) -> str:
    """
    Read the template data query files.
    Replace the 'period' keyword in the template the `period` kwarg
    """
    with open(fname, "rb") as fp:
        query_str = fp.read().decode("utf8")
    return query_str

clin_tables = ["med", "vatska", "vikt", "respirator", "pressure", "lab", "fio2"]
all_data_tables = ["overview", "takecare"]+clin_tables+[ "monitorlf", "monitorhf"]
all_data_tables2 = ["takecare"]+clin_tables+[ "monitorlf", "monitorhf"]

ref_cols = ["\""+s+"\"" for s in ['ids__uid', 'ids__interval', 'interval__raw', 'interval__start', 'interval__end']]


query_size = "select {} from "\
            "(select * from {} "\
            "where ids__interval = '{}') as foo;"


def get_update_status(start_):
    return [html.P(gdate()), html.P("({} ms)".format(round(((datetime.now() - start_).total_seconds()) * 1000, 1)))]


def define_col_db_size_query(k, con):
    return ", ".join(list(map(lambda s: "pg_column_size(" + s + ") as " + s, get_colnames(k, con))))


def get_size_interval(ids__interval, con):
    dout = {k: pd.read_sql(query_size.format(define_col_db_size_query(k, con), k, ids__interval), con).sum(1) for k in all_data_tables2}
    dout = {k: int(v.values[0]) for k, v in dout.items() if not v.empty}
    return dout


def get_size_patient(ids__uid, con):
    return None


def get_pat_intervals(thepatid, con):
    return pd.read_sql("select ids__interval from view__interv_all where ids__uid like '{}'".format(thepatid),
                            con).values.reshape(-1).tolist()


def run_select_queries(select_queries, engine):
    data = {}
    with engine.connect() as con:
        for k, v in select_queries.items():
            df = pd.read_sql(v, con)
            non_empty_col = (df.notna().sum(0) != 0)
            data[k] = df[non_empty_col.index.values[non_empty_col.values].tolist()]
    return data
