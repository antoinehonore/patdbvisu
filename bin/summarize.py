import argparse
import pandas as pd
from parse import parse
import os
from utils_tbox.utils_tbox import date_fmt, pidprint, gdate, write_pklz, read_pklz
from utils_db.utils_db import get_engine, get_dbcfg, run_query,get_pat_feats, run_query,get_pat_labels
from sqlalchemy import text
import sys
import numpy as np
from collections import ChainMap
from multiprocessing import Pool
from functools import partial

def summarize_patdata(ids__uid, wlen_min=10, cfg_fname="cfg/db.cfg", verbose=0, cache_dir="cache"):
    if verbose>0:
        pidprint("Processing",ids__uid,"...")

    out_fname = os.path.join(cache_dir, ids__uid + "_frame_count_" + str(wlen_min)+"min.pklz")
    if not os.path.exists(out_fname):
        out_features_fname = os.path.join(cache_dir, ids__uid + "_features_" + str(wlen_min)+"min.pklz")
        if not os.path.exists(out_features_fname):
            dbcfg = get_dbcfg(cfg_fname)
            engine = get_engine(verbose=verbose, **dbcfg)

            X = get_pat_feats(ids__uid, engine, wlen_min=wlen_min)
            X,_ = get_pat_labels(ids__uid, X.set_index("timeline"), 
                    dict(restrict_bw=24, restrict_fw=0, max_gap_days=14, train_margin_type="None"),
                    {"targets":["los"]}, 
                    {"drop_missing":False},
                cfg_fname=cfg_fname)
            write_pklz(out_features_fname,X)
        else:
            X = read_pklz(out_features_fname)
        # List the target and feature names, assuming signals "spo2","rf","btb"
        targets_columns = [s for s in X.columns if s.startswith("target__")]
        feats_columns = {thesig: [s for s in X.columns if s.startswith("feats__"+thesig)] for thesig in ["spo2","rf","btb"]}

        # Find whether a frame (a row) contains data (rather than NaN) for the features of each signals, assuming that all features from a signal are NaN if the 
        # signal is missing. 
        frames_with_features_per_signal = {thesig: X[cols].isna().sum(1)==0 for thesig,cols in feats_columns.items()}
        frames_with_features_allsignals = np.concatenate([v.values.reshape(-1,1) for v in frames_with_features_per_signal.values()],axis=1).sum(1)==3

        # Count the number of frames with features and a positive label, for each possible target event and signal
        n_pos_frames_per_evt_signal = {evt: {thesig: X[evt][frames_with_features_per_signal[thesig].values].sum() for thesig in ["spo2","rf","btb"] } 
                                        for evt in targets_columns}
        
        n_pos_frames_per_evt_allsignal = {"allsignals__"+evt: X[evt][frames_with_features_allsignals].sum() for evt in targets_columns}
        
        # CTRL frames are frames not 24h prior to any event
        ctrl_frames = X[targets_columns].sum(1) == 0
        n_ctrl_allsignals = {"all_signals__ctrl":(ctrl_frames.values & frames_with_features_allsignals).sum()}
        n_ctrl_frames_per_signal = {thesig + "__ctrl": X[ctrl_frames.values & frames_with_features_per_signal[thesig].values].shape[0] for thesig in ["spo2", "rf", "btb"] }
        
        # Concat all the info in a list of dictionary 
        a = [{thesig + "__" + evt : value for thesig, value in n_pos_frames_per_evt_signal[evt].items()} for evt in n_pos_frames_per_evt_signal.keys()]\
            +[n_ctrl_frames_per_signal] \
            +[n_pos_frames_per_evt_allsignal, n_ctrl_allsignals]
        
        # Make it one dictionary
        tedict = dict(ChainMap(*a))
        outdict = {ids__uid: tedict}
        write_pklz(out_fname, outdict)
    else:
        outdict = read_pklz(out_fname)
    return outdict


parser = argparse.ArgumentParser()
parser.add_argument("-wlen",type=int,help="Window length in minutes",default=10)
parser.add_argument("-j",type=int,help="Number of jobs",default=1)
parser.add_argument("-v",type=int,help="Verbosity (int)",default=0)
parser.add_argument("-cache",type=str,help="Cache directory",default="cache")


if __name__ == "__main__":
    args = parser.parse_args()

    wlen_min = args.wlen
    verbose = args.v
    n_jobs = args.j
    cache_dir = args.cache
    os.makedirs(cache_dir, exist_ok=True)

    cfg_fname = "cfg/db.cfg"
    dbcfg = get_dbcfg(cfg_fname)
    engine = get_engine(verbose=verbose, **dbcfg)
    
    query = "select ov.ids__uid as \"ids__uid2\", ov.bw, ov.ga_w, ov.sex,ov.birthdate, vuh.* from view__uid_has vuh, overview ov\
        where neo=1 and takecare=1 and ((spo2=1)or(btb=1)or(rf=1)) and vuh.ids__uid =ov.ids__uid "

    demo_data = run_query(query, engine)    
    df = run_query(query, engine, verbose=verbose).drop(columns=["ids__uid2"])
    if verbose:
        pidprint("Number of patients to process:", df.shape[0])
    all_ids = df["ids__uid"].values
    all_pat_summaries = []
    fp = partial(summarize_patdata, wlen_min=wlen_min, verbose=verbose, cfg_fname=cfg_fname, cache_dir=cache_dir)
    
    if n_jobs == 1:
        all_pat_summaries = list(map(fp, all_ids))
    else:
        with Pool(processes=n_jobs) as pool:
            all_pat_summaries = pool.map(fp, all_ids)
    
    for i, ids__uid in enumerate(all_ids):
        for k, v in all_pat_summaries[i][ids__uid].items():
            df.loc[i, k] = v if v>0 else np.nan
    df.set_index("ids__uid", inplace=True)
    
    keep_cols = ["sex","ga_w","preterm","bw","vlbw","birthdate"]+[s for s in df.columns if (s.endswith("los") or s.endswith("ctrl"))]
    keep_cols = df.columns
    df[keep_cols].to_csv("summary_neo_all_wlen_{}min.csv".format(wlen_min))
    
