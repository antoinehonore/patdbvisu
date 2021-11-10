import argparse
import os
import subprocess
from patdbvisu.src.utils import register_ids, pidprint, get_dbcfg,get_engine
from patdbvisu.src.patdisp import is_pn, init_hash_fun, format_pn

import sys


def decrypt_file(fname):
    cmd = "gpg -d --passphrase-file=/opt/psql/gpg_antoine_pfile.txt --yes --batch {} 2>/dev/null".format(fname)
    s = subprocess.check_output(cmd, shell=True).decode("utf8")
    return s


def read_fname(fname):
    if ".gpg" in os.path.basename(fname):
        s = decrypt_file(fname)

    else:
        with open(fname, "rb") as fp:
            s = fp.read().decode("utf8")

    if s[:2] != "ID":
        lines = list(map(lambda x: x.split(";"), s.strip()[1:].split("\r\n")))
    else:
        lines = list(map(lambda x: x.split(";"), s.strip().split("\n")))

    return lines


def read_map(fname):
    lines = read_fname(fname)
    d = pd.DataFrame(data=lines[1:], columns=lines[0])
    rootdir = os.path.basename(os.path.dirname(fname))
    d["ID"] = rootdir + "_pat" + d["ID"]
    #fhash = init_hash_fun()
    #f = lambda s: fhash(format_pn(s.strip()))
    #format_pn("9302013231")
    #d["Personnummer"] = d["Personnummer"].astype(str).apply(f)
    return d.set_index("ID")


#def read_pnuidmap(fname):
#    if os.path.isfile(fname+".gpg"):
#        c = decrypt_file(fname+".gpg").strip()
#        out = {ll[0]: ll[1] for ll in [l.split(":") for l in c.split("\n")]}
#    else:
#        out = {}
#    return out


import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str)
import numpy as np

if __name__ == "__main__":
    args = parser.parse_args()
    fname = args.i
    #themap = read_map(fname)
    cfg_root = "cfg"
    if not os.path.isfile(fname):
        pidprint(fname, "does not exist", flag="error")
        sys.exit(1)

    if fname.endswith(".csv"):
        df = pd.read_csv(fname, sep=";")
        ext = ".csv"
    elif fname.endswith(".xlsx"):
        df = pd.read_excel(fname)
        ext = ".xlsx"
    elif os.path.basename(fname) == "PatientsMapping.txt.gpg":
        df = read_map(fname)
        ext = ".txt.gpg"

    else:
        pidprint("unknown filetype:{}".format(fname), flag="error")
        sys.exit(0)

    for k in ["Personnummer", "personnummer", "PN", "pn"]:
        if k in list(df):
            fhash = init_hash_fun()

            df[k] = df[k].apply(lambda s: format_pn(s.strip().replace(" ", "")))
            not_pns = df[k].apply(lambda s: not is_pn(str(s)))

            if not_pns.any():
                pidprint(
                    "Some entries in the {} column are not formatted as PNs:\n{}".format(k, df[k][not_pns]),
                    flag="error")
                # sys.exit(1)

            new_entries = {fhash(s): s for s in np.unique(df.loc[~not_pns, k].values)}

            pidprint("{} entries found in file.".format(len(new_entries)), flag="info")

            dbcfg = get_dbcfg(os.path.join(cfg_root, "pnuid.cfg"))
            engine = get_engine(verbose=False, **dbcfg)
            with engine.connect() as con:
                register_ids(new_entries, con)
