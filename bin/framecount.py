import argparse
import pandas as pd
from parse import parse
import os
from utils_plots.utils_plots import better_lookin
from utils_tbox.utils_tbox import date_fmt, pidprint, gdate, write_pklz, read_pklz
from utils_db.utils_db import get_engine, get_dbcfg, run_query,get_pat_feats, run_query,get_pat_labels
from sqlalchemy import text
import sys
import numpy as np
from collections import ChainMap
from multiprocessing import Pool
from functools import partial
import argparse
import matplotlib.pyplot as plt


def summarize_patdata(ids__uid, wlen_min=10, cfg_fname="cfg/db.cfg", verbose=0, cache_dir="cache"):
    if verbose > 0:
        pidprint("Processing", ids__uid, "...")

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
parser.add_argument("--tikz",action='store_true',help="Draw tikz flowcharts",default=False)


if __name__ == "__main__":
    args = parser.parse_args()

    wlen_min = args.wlen
    verbose = args.v
    n_jobs = args.j
    cache_dir = args.cache
    tikz = args.tikz
    
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(os.path.join(cache_dir, "patlist"), exist_ok=True)
    os.makedirs(os.path.join(cache_dir, "plots"), exist_ok=True)
    os.makedirs(os.path.join(cache_dir, "tikz"), exist_ok=True)

    cfg_fname = "cfg/db.cfg"
    fname = "summary_neo_all2024_wlen_10min.csv"
    #fname = "summary_neo_all_wlen_{}min.csv".format(wlen_min)
    df = pd.read_csv(fname)

    # Count of events
    all_events = ["neo_adverse","los","eos","cps_los","cns_los","cns_eos","cps_eos","infection","bleeding","lung_bleeding","infection","cns_infection","sro","abdominal_nec", "pneumonia","brain_ivh_stage_3_4","lung_bleeding"]
    a = []
    for theevent in all_events:
        # Patients with positive frames for the event
        npat_not_evt_tot = int((df[theevent]==0).sum())

        npat_evt_tot = int(df[theevent].sum())

        df_evt = df[(df[theevent]==1) & (df["allsignals__target__{}".format(theevent)].notna())].sort_values("allsignals__target__{}".format(theevent))
        # Write the patient list to file:
        csv_s = "\n".join(df_evt[df_evt[theevent]==1]["ids__uid"].unique().tolist())
        with open(os.path.join(cache_dir,"patlist","{}_evt_pat_list.csv".format(theevent)),"w",encoding="utf8") as fp:
            fp.write(csv_s)

        df_evt_ctrl = df[(df[theevent]==1) & (df["all_signals__ctrl"].notna())]
        df_evt_not = df[(df[theevent]==0) & (df["all_signals__ctrl"].notna())]
        csv_s = "\n".join(df_evt_not[df_evt_not[theevent]==0]["ids__uid"].unique().tolist())
        with open(os.path.join(cache_dir,"patlist","{}_notevt_pat_list.csv".format(theevent)),"w",encoding="utf8") as fp:
            fp.write(csv_s)

        npat_not_ctrl = df_evt_not["ids__uid"].unique().shape[0]
        n_frames_not_ctrl = int(df_evt_not["all_signals__ctrl"].sum())


        # Plot the frame count for each patient
        fig, ax = plt.subplots(figsize=(8, 4))
        rect = ax.barh(df_evt["ids__uid"], df_evt["allsignals__target__{}".format(theevent)], color="darkgreen")
        ax.bar_label(rect,df_evt["allsignals__target__{}".format(theevent)].astype(int),fontsize=10)
        ax.set_xlabel("# positive {}min frames".format(wlen_min))

        npat_pos = df_evt["ids__uid"].unique().shape[0]
        n_frames_pos = int(df_evt["allsignals__target__{}".format(theevent)].sum())

        npat_ctrl_pos = df_evt_ctrl["ids__uid"].unique().shape[0]
        n_frames_ctrl = int(df_evt_ctrl["all_signals__ctrl"].sum())

        ax.set_yticklabels(df_evt["ids__uid"].apply(lambda s:s[:10]),ha='right')
        ax.set_title("{} event\n # Patients: {}/{}\n # {} minutes frames: {}".format(theevent, npat_pos, npat_evt_tot, wlen_min,n_frames_pos))

        better_lookin(ax, grid=False, fontsize=12)
        plt.tight_layout()

        fig.savefig(os.path.join(cache_dir, "plots", "{}_patients_{}min_frame_count.pdf".format(theevent,wlen_min)))

        a.append([theevent,npat_evt_tot, npat_pos, n_frames_pos, npat_ctrl_pos, n_frames_ctrl, npat_not_evt_tot, npat_not_ctrl, n_frames_not_ctrl])

    col_names = ["theevent","npat_evt_tot", "npat_pos", "n_frames_pos", "npat_ctrl_pos", "n_frames_ctrl","npat_not_evt_tot","npat_not_ctrl","n_frames_not_ctrl"]
    dfout = pd.DataFrame(a, columns=col_names)
    dfout.to_excel(os.path.join(cache_dir, "evt_data_info.xlsx"))
    ntot_pat = [_a[1]+_a[6] for _a in a]
    assert(all([tot_count == ntot_pat[0] for tot_count in ntot_pat]))
    ntot_pat = ntot_pat[0]
    if tikz:
        for i,_a in enumerate(a):
            with open("tikz_template.tex","r",encoding="utf8") as fp:
                stikz = fp.read()
            stikz = stikz.replace("npat_tot","{}".format(ntot_pat))
            for thename,thedata in zip(col_names,_a):
                stikz = stikz.replace(thename,str(thedata).replace("_"," "))
            outfname = os.path.join(cache_dir, "tikz", "tikz_patcount_{}.tex".format(_a[0]))
            with open(outfname, "w", encoding="utf8") as fp:
                fp.write(stikz)
    
    print()