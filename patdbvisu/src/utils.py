from datetime import datetime
import os
from sqlalchemy import create_engine
import sys
import pandas as pd
from .styles import style_tbl
import json
from dash import dcc, html
from dash import dash_table as dt


date_fmt = "%Y-%m-%d %H:%M:%S"


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


def read_passwd(username: str = "remotedbuser", root_folder: str = ".") -> str:
    """
    Read `username` password file from the `root_folder`.
    """
    with open(os.path.join(root_folder, "{}_dbfile.txt".format(username)), "r") as f:
        s = f.read().strip()
    return s



def register_ids(themap, cfg_root="cfg"):
    dbcfg = get_dbcfg(os.path.join(cfg_root, "pnuid.cfg"))

    engine = get_engine(verbose=False, **dbcfg)

    with engine.connect() as con:
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


def run_select_queries(select_queries, engine):
    data = {}
    with engine.connect() as con:
        for k, v in select_queries.items():
            df = pd.read_sql(v, con)
            non_empty_col = (df.notna().sum(0) != 0)
            data[k] = df[non_empty_col.index.values[non_empty_col.values].tolist()]
    return data

