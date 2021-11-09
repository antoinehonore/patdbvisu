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


def read_map(fname):
    if os.path.isfile(fname+".gpg"):
        c = decrypt_file(fname+".gpg").strip()
        out = {ll[0]: ll[1] for ll in [l.split(":") for l in c.split("\n")]}
    else:
        out = {}
    return out


import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    fname = args.i
    #themap = read_map(fname)
    cfg_root = "cfg"

    if fname.endswith(".csv"):
        df = pd.read_csv(fname, sep=";")
        ext = ".csv"
    elif fname.endswith(".xlsx"):
        df = pd.read_excel(fname)
        ext = ".xlsx"
    else:
        pidprint("unknown filetype:{}".format(fname), flag="error")
        sys.exit(0)

    for k in ["Personnummer", "personnummer", "PN", "pn"]:
        if k in list(df):
            fhash = init_hash_fun()

            not_pns = df[k].apply(lambda s: (is_pn(str(s).strip().replace(" ", "")) is None))
            if not_pns.any():
                pidprint(
                    "{}: Some entries in the {} column are not formatted as PNs:\n{}".format(fhash, k, df[k][not_pns]),
                    flag="error")
                sys.exit(1)

            new_entries = {fhash(format_pn(s.strip())):s for s in df.loc[~not_pns, k].values}

            new_entries = df.loc[~not_pns, ("ids__uid", k)].drop_duplicates().values

            pidprint("{} entries found in file.".format(len(new_entries)), flag="info")

            dbcfg = get_dbcfg(os.path.join(cfg_root, "pnuid.cfg"))
            engine = get_engine(verbose=False, **dbcfg)
            with engine.connect() as con:
                register_ids({k: v for k, v in new_entries}, con)


    print("")

