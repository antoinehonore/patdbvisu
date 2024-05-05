import argparse
import os
from utils_db.anonym import register_ids
from utils_db.utils_db import get_dbcfg, get_engine, pidprint
from utils_db.anonym import is_pn, init_hash_fun, format_pn
from utils_tbox.io import read_map
import sys
import numpy as np
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str)
parser.add_argument("--mapped", action='store_true', help="Covnert back to PNs", default=False)
parser.add_argument("--register", action='store_true', help="Covnert back to PNs", default=False)


if __name__ == "__main__":
    args = parser.parse_args()
    fname = args.i
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

    if args.register:
        for k in ["Personnummer", "personnummer", "PN", "pn"]:
            if k in list(df):
                fhash = init_hash_fun()

                df[k] = df[k].apply(lambda s: format_pn(s.strip().replace(" ", "")))
                not_pns = df[k].apply(lambda s: not is_pn(str(s)))
                
                if not_pns.any():
                    pidprint(
                        "Some entries in the {} column are not formatted as PNs:\n{}".format(k, df[k][not_pns]),
                        flag="error")

                new_entries = {fhash(s): s for s in np.unique(df.loc[~not_pns, k].values)}
                # with open("david/new_ids_mapping.csv","w") as fp:
                # print("\n".join([",".join([v,k]) for k,v in new_entries.items()]),file=fp)
                pidprint("{} entries found in file.".format(len(new_entries)), flag="info")

                dbcfg = get_dbcfg(os.path.join(cfg_root, "pnuid.cfg"))
                engine = get_engine(verbose=False, **dbcfg)
                with engine.connect() as con:
                    register_ids(new_entries, con)

    if args.mapped:
        for k in ["ids__uid"]:
            if k in list(df):
                dbcfg = get_dbcfg(os.path.join(cfg_root, "pnuid.cfg"))
                engine = get_engine(verbose=False, **dbcfg)
                with engine.connect() as con:
                    dfout = pd.read_sql("select * from themap where ids__uid in ({})".format(", ".join(list(map(lambda s: "\'"+s+"\'", df["ids__uid"].values.tolist())))), con) #df["ids__uid"].apply(search_id)#(df.values[0][0])
                
                dfout.to_excel(fname.replace(".xlsx", "_mapped.xlsx"), index=False)
