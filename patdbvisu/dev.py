import os
from utils_db.utils_db import get_engine, get_hf_data, get_dbcfg, run_query
import pandas as pd
import socket
from datetime import datetime
#import matplotlib.pyplot as plt


if __name__ == "__main__":
    dbcfg = get_dbcfg("cfg/db.cfg")
    engine = get_engine(verbose=False, **dbcfg)
    rdir = ".." if socket.gethostname() == "cmm0576" else "."

    sizes_fname = os.path.join(rdir, "thesizes.csv")

    if not os.path.isfile(sizes_fname):
        dfsizes = run_query("select * from view__monitor_data_size",
                      engine, verbose=2)

        dfsizes.to_csv(sizes_fname, sep=";")

    else:
        dfsizes = pd.read_csv(sizes_fname, sep=";")

    dfsizes.drop(columns=["Unnamed: 0"], inplace=True)
    dfsizes.columns = dfsizes.columns[:-1].values.tolist()+["datasize"]
    dfsizes['interval__start'] = pd.to_datetime(dfsizes['interval__start'])
    dfsizes.set_index('interval__start', inplace=True)
    dfsizes = dfsizes["datasize"].resample(pd.Timedelta(1, "day")).sum()
    dfsizes = pd.DataFrame(index=dfsizes.index,data=dfsizes.values,columns=["datasize"])

    ref_start = pd.to_datetime(["20170101"], format="%Y%m%d")[0]
    ref_end = datetime.now()

    dfsizes = dfsizes[(dfsizes.index > ref_start) & (dfsizes.index < ref_end)]

    full_index = pd.date_range(ref_start, ref_end, freq=pd.Timedelta(1, "day"))
    dout = pd.DataFrame(index=full_index, columns=["datasize"])
    dout.index.name = "interval__start"

    dout.reset_index(inplace=True)
    dfsizes.reset_index(inplace=True)
    #dfsizes["datasize"] = dfsizes["datasize"] - dfsizes["datasize"].min()*2

    dfplot = pd.concat([dout, dfsizes], axis=0).fillna(0).set_index("interval__start").sort_index().resample(pd.Timedelta(1, "day")).sum()
    d_gb = dfplot.values/1024/1024/1024

    #plt.close()
    #plt.plot(dfplot.index, d_gb)

    intervs = ["db3765793e4fa989c5ccf2575888deec874fbbe3d86fbcc6be0129879a55fb15",
            "eabd26deb24719d8f637dcbac5825d553bbb4d72fb84f0508002a33bc8a103b9",
            "9b0cbee8f928fhtop39fcf093a8524644194898987667198bf62810fd8eb5411ec33",
            "f991dbaabfdf1abf2a30a8fe7c705d12b52fb3e8c77791d921fc35cded11c622",
            "5c9ad41786729f980011a848ea919ede8b30efad7e8cd56e25e58ce98ba1d7d4",
            "f7526b4143ab3754fbd028f04f67954a8a931db8915f634c9b8e7e264e495a1a",
            "f868bfc2de846e1ac2db38da60896e7c1261e700d3f05f44f40781f46560423c",
            "c8160c91b7f97bd56296b529d847825821f4720389bab61510e61fe2353f3e37"]

    dfmon = get_hf_data(intervs, engine, verbose=2)
