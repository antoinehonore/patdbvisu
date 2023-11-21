import argparse
import pandas as pd

from parse import parse
import os
from utils_plots.utils_plots import better_lookin
from utils_tbox.utils_tbox import date_fmt, pidprint, gdate, write_pklz, read_pklz
from utils_db.utils_db import get_engine, get_dbcfg, run_query,get_pat_feats, run_query,get_pat_labels,ids2str
from sqlalchemy import text
import sys
import numpy as np
from collections import ChainMap
from multiprocessing import Pool
from functools import partial
import argparse
import matplotlib.pyplot as plt
from tableone import TableOne


parser = argparse.ArgumentParser()
parser.add_argument("-wlen",type=int,help="Window length in minutes", default=10)
parser.add_argument("-j",type=int,help="Number of jobs", default=1)
parser.add_argument("-i",type=str,help="Input patient list", default=1)
parser.add_argument("-outdir",type=str,help="Output directory", default=None)

parser.add_argument("-v",type=int,help="Verbosity (int)", default=0)
parser.add_argument("-cache",type=str,help="Cache directory", default="cache")
parser.add_argument("--tikz",action='store_true',help="Draw tikz flowcharts", default=False)
parser.add_argument("--tableone",action='store_true',help="Compute and store a descriptive table", default=False)
parser.add_argument("--parameterdata",action='store_true',help="Retrieve and store the parameter data of all patients.", default=False)

if __name__ == "__main__":
    args = parser.parse_args()

    wlen_min = args.wlen
    verbose = args.v
    n_jobs = args.j
    cache_dir = args.cache
    tikz = args.tikz
    input_fname = args.i
    compute_tableone = args.tableone
    parameter_data = args.parameterdata
    outdir = args.outdir if not (args.outdir is None) else input_fname.replace(".csv","")
    os.makedirs(outdir,exist_ok=True)

    study_pat_df = pd.read_csv(input_fname)
    study_pat_list = study_pat_df.values.reshape(-1).tolist()
    study_pat_str = ids2str(study_pat_list)

    cfg_fname = "cfg/db.cfg"
    dbcfg = get_dbcfg(cfg_fname)
    engine = get_engine(verbose=verbose, **dbcfg)
    
    query = "select vuh.*,ov.birthdate,ov.ga_w,ov.sex,ov.bw from view__uid_has vuh, overview ov where ov.ids__uid in ({}) and ov.ids__uid = vuh.ids__uid".format(study_pat_str)

    demo_data = run_query(query, engine)
    demo_data["in_database"] = 1

    study_db_overview = pd.concat([study_pat_df.set_index("ids__uid"), demo_data.set_index("ids__uid")], axis=1)
    study_db_overview["in_database"].fillna(0,inplace=True)
    study_db_overview.reset_index().set_index(["ids__uid", "birthdate"], inplace=True)
    #demo_data.set_index(["ids__uid","birthdate"]).sum(0).to_excel("{}_in_database_desc.xlsx".format(input_fname.replace(".csv","")))

    if compute_tableone:
        study_db_overview["los"] = ["los" if v == 1 else "" for v in study_db_overview["los"]]
        study_db_overview["abdominal_nec"] = ["nec" if v == 1 else "" for v in study_db_overview["abdominal_nec"]]
        study_db_overview["Condition"] =  (study_db_overview["los"] + "/" + study_db_overview["abdominal_nec"]).replace({"/":"Healthy","/nec":"NEC only", "los/":"LOS only", "los/nec":"LOS \& NEC"})

        categorical = ["sex"] #+['los',"abdominal_nec"]
        columns = ["ga_w", "bw", "sex"] #+['los',"abdominal_nec"]
        groupby = ["Condition"]
        mytable = TableOne(study_db_overview, columns=columns, categorical=categorical, groupby=groupby,rename={"ga_w":"GA [w]","bw":"BW [g]","sex":"Sex"},pval=True)
        mytable.to_latex("{}_in_database_desc.tex".format(input_fname.replace(".csv","")),column_format="m{2cm}")
    
    if parameter_data:
        from utils_db.utils_db import prefix_suffix_str, get_interv_query, set_lf_query, valid_signames, merge_sig, clean_sig
        

        def export_raw_lf_data(ids__uid, outdir=".", verbose=0):
            outfname = os.path.join(outdir, "vitalsigns_{}.csv.gz".format(ids__uid))

            if verbose:
                pidprint("Processing",ids__uid)
            if not os.path.exists(outfname):
                cfg_fname = "cfg/db.cfg"
                dbcfg = get_dbcfg(cfg_fname)
                engine = get_engine(verbose=verbose, **dbcfg)
                
                s_uid = get_interv_query(ids__uid)
                with engine.connect() as con:
                    the_intervals = list(map(prefix_suffix_str, pd.read_sql(s_uid, con).values.reshape(-1).tolist()))
                if len(the_intervals) >0:
                    s_interv = set_lf_query(the_intervals)
                    dfmonitor = run_query(s_interv, engine, verbose=verbose)
                    
                    allsigs = {
                        k: merge_sig(clean_sig(dfmonitor[v].dropna(axis=1, how='all')).values.reshape(-1), k, date_col="timestamp").rename_axis(k+"_timeline").reset_index() for
                        k, v in valid_signames.items()
                    }

                    # Export
                    dout = pd.concat(list(allsigs.values()), axis=1, ignore_index=True)
                else:
                    if verbose:
                        pidprint("Empty vital signs data.",flag="warning")
                    dout=pd.DataFrame()
                dout.to_csv(outfname, sep=";", compression="infer")
            else:
                dout = pd.read_csv(outfname, sep=";", compression="infer")
            return
        
        out = list(map(partial(export_raw_lf_data, outdir=outdir, verbose=verbose), study_pat_list))

        print("")
