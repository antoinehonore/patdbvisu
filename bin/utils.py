from datetime import datetime
import os
from sqlalchemy import create_engine
import sys
import pandas as pd

import json
date_fmt="%Y-%m-%d %H:%M:%S"

clin_tables = ["med", "vatska", "vikt", "respirator", "pressure", "lab", "fio2"]
all_data_tables = ["overview", "takecare"]+clin_tables+[ "monitorlf", "monitorhf"]
all_data_tables2 = ["takecare"]+clin_tables+[ "monitorlf", "monitorhf"]

ref_cols = ["\""+s+"\"" for s in ['ids__uid', 'ids__interval', 'interval__raw', 'interval__start', 'interval__end']]


query_size = "select {} from "\
            "(select * from {} "\
            "where ids__interval = '{}') as foo;"


def define_col_db_size_query(k, con):
    return ", ".join(list(map(lambda s: "pg_column_size(" + s + ") as " + s, get_colnames(k, con))))


def get_size_interval(ids__interval, engine):
    with engine.connect() as con:
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


def get_colnames(k, con):
    df = pd.read_sql("select column_name   FROM information_schema.columns "\
                   " WHERE table_schema = \'public\'    AND table_name   = \'{}\' ".format(k),
                   con)
    return list(map(lambda s: "\""+s+"\"", df["column_name"].values.tolist()))


def gdate(date_fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.now().strftime(date_fmt)


def mon_sig_name_fix(s):
    # Fix names
    return s.lower().replace(",", "").replace("(", "").replace(".", "").replace(")", "")\
        .replace(" ", "__").replace("%", "perc")


def pidprint(*arg, flag="status"):
    """
    Behaves like builtin `print` function, but append runtime info before writing to stderr.
    The runtime info is:

    - pid
    - datetime.now()
    - flag, specified as a keyword
    """
    print("[{}] [{}] [{}]".format(os.getpid(),datetime.now(), flag)," ".join(map(str, arg)), file=sys.stderr)
    return


def read_passwd(username: str = "remotedbuser", root_folder: str = ".") -> str:
    """
    Read `username` password file from the `root_folder`.
    """
    with open(os.path.join(root_folder, "{}_dbfile.txt".format(username)), "r") as f:
        s = f.read().strip()
    return s


def get_dbcfg(fname):
    with open(fname,"rb") as fp:
        dbcfg=json.load(fp)
    return dbcfg

def read_query_file(fname: str) -> str:
    """
    Read the template data query files.
    Replace the 'period' keyword in the template the `period` kwarg
    """
    with open(fname, "rb") as fp:
        query_str = fp.read().decode("utf8")
    return query_str

def get_engine(username: str = "remotedbuser", root_folder: str = ".", nodename: str = "client", schema=None,dbname:str="remotedb", verbose=False):
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
