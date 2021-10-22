from datetime import datetime
import os
from sqlalchemy import create_engine
import sys

import json
date_fmt="%Y-%m-%d %H:%M:%S"


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
