import os

from utils_db.utils_db import get_engine, get_hf_data, get_dbcfg, run_query
import pandas as pd
import socket
from datetime import datetime

from patdbvisu.src.patdisp import get_lf_data
import matplotlib.pyplot as plt

if __name__ == "__main__":
    dbcfg = get_dbcfg("cfg/db.cfg")
    engine = get_engine(verbose=False, **dbcfg)
    rdir = ".." if socket.gethostname() == "cmm0576" else "."

    get_lf_data(["\'fc7cad821ee39d813dcc2893a680288390d07c011fb7e029e870930e401dee05\'",
                 "\'8d68b12880e12f9d8b131af503c9626c0a6864c8b093ce2221391461f115216d\'",
                 "\'909a21eaf2720305f4d897ff5665419781bc20d4a42e0f34565d4bbbebbe183e\'"], engine, Ts="10T",disp_all_available=False)

    sizes_fname = os.path.join(rdir, "thesizes.csv")

    if not os.path.isfile(sizes_fname):
        dfsizes = run_query("select * from view__monitor_data_size",
                      engine, verbose=2)

        dfsizes.to_csv(sizes_fname, sep=";")

    else:
        dfsizes = pd.read_csv(sizes_fname, sep=";")

    dfsizes.drop(columns=["Unnamed: 0"], inplace=True)
    #    dfsizes.columns = dfsizes.columns[:-1].values.tolist()+["datasize"]
    dfsizes['interval__start'] = pd.to_datetime(dfsizes['interval__start'])
    d = {}
    dout = {}
    dfplot = {}
    d_gb = {}
    dfsizes.set_index('interval__start', inplace=True)
    ref_start = pd.to_datetime(["20170101"], format="%Y%m%d")[0]
    ref_end = datetime.now()
    full_index = pd.date_range(ref_start, ref_end, freq=pd.Timedelta(1, "day"))

    for k in ["lf", "hf"]:
        d[k] = dfsizes[dfsizes["type"] == k]
        d[k] = d[k]["datasize"].resample(pd.Timedelta(1, "day")).sum()
        d[k] = pd.DataFrame(index=d[k].index,
                            data=d[k].values,
                            columns=["datasize"])

        d[k] = d[k][(d[k].index > ref_start) & (d[k].index < ref_end)]

        dout[k] = pd.DataFrame(index=full_index, columns=["datasize"])
        dout[k].index.name = "interval__start"

        dout[k].reset_index(inplace=True)
        d[k].reset_index(inplace=True)

        dfplot[k] = pd.concat([dout[k], d[k]], axis=0).fillna(0).set_index("interval__start").sort_index().resample(pd.Timedelta(1, "day")).sum()
        d_gb[k] = dfplot[k].values/1024/1024/1024

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    colors = {"hf":"darkblue","lf":"darkred" }
    ax1.plot(dfplot["hf"].index,d_gb["hf"],color=colors["hf"])
    ax1.tick_params(axis='y', labelcolor=colors["hf"])
    ax2 = ax1.twinx()

    ax2.plot(dfplot["lf"].index, d_gb["lf"],color=colors["lf"])
    ax2.tick_params(axis='y', labelcolor=colors["lf"])
    print("")