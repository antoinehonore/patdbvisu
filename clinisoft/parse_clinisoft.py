"""
Created on 25 Oct 2019

@author: anthon
"""
import hashlib, uuid
import os
import sys
import pandas as pd
import numpy as np
import argparse
from parse import parse
from functools import partial
from multiprocessing.dummy import Pool
from utils_tbox.utils_tbox import pidprint
from utils_db.anonym import init_hash_fun, format_pn
import unidecode


fix_clin_data = {
    19335:
        {
            "CIRK":
                {"Vikt": {940:0.940}
                 }
        },
    21083:
        {
            "CIRK":
                {"Vikt": {1545:1.545}
                 }
        },
    20215:
    {
        "CIRK":
            {"Vikt": {0.425:0.725}
             }
    },
    20633:
    {
        "CIRK":
            {"Vikt":
                 {0.752:np.nan}
             }
   },
    21122:
        {
                "CIRK":
                    {"Vikt":

                         {9.44:np.nan}
                    }
        },
    21325:
        {
            "CIRK":
                {"Vikt":
                     {18.73: 1.873}
                 }
        },
}


def pressure_sel(var):
    """Finds the pressure related variables."""
    return any([x in var for x in ["pfi", "pmean", "pinsp", "luftv", "peep", "ppeak", "luftv_tr_med",\
                            "luftv_trp_u_", "hfo_ampl"]]) or var == "cpap"


def append_clinid(d, clinid):
    """Append the integer clinid on the left hand side of dataFrame d with column name 'clinisoftid'."""
    out = pd.concat(
        [pd.DataFrame(index=d.index, columns=["clinid"], data=clinid * np.ones((d.shape[0])).astype(int)), d
         ], axis=1)
    return out


def format_fname(fname_out, thetype):
    return fname_out.replace(".csv", "_{}.csv".format(thetype))


def format_str(s):
    s_c = ''.join(e if e.isalnum() else '_' for e in s.replace(" ", "__"))
    out = unidecode.unidecode(s_c).lower()
    return out

def read_injection(df_, sheet_name="LM_Givet"):
    med_df = df_[sheet_name]
    med_df.rename(columns=lambda x: x.strip(), inplace=True)
    if "DateTime" in list(med_df):
        med_df["Tid"] = med_df["DateTime"]

    if "Hastighet" in list(med_df):
        med_df["Summa"] = med_df["Hastighet"]

    if not ("Tid" in list(med_df)):
        return create_empty(["{}__{}".format(sheet_name, "empty")])

    med_df.set_index('Tid', inplace=True)
    med_df = med_df[med_df["Status"] != 782]

    med_units = med_df[["Namn", "Enhet"]].values
    uniq_medunits = np.unique(["_".join(t) for t in med_units])
    uniq_medunits_tuple = [t.split("_") for t in uniq_medunits]

    DF = [pd.DataFrame(index=med_df[med_df["Namn"] == t_uple[0]].index,
                       data=med_df[med_df["Namn"] == t_uple[0]]["Summa"].values,
                       columns=["{}__{}".format(sheet_name, format_str(s_tr))])
          for s_tr, t_uple in zip(uniq_medunits, uniq_medunits_tuple)]

    DF_ = [d.loc[(d != 0).any(axis=1)] for d in DF]

    all_meds = pd.concat(DF_, axis=0, sort=True).sort_index()
    all_meds.rename(columns=lambda x: format_str(x), inplace=True)
    return all_meds


def check_df(df, verbose=True):
    """Sometimes the 1st row is not te expected header."""
    for k in list(df):
        if not any("Tid" in x for x in list(df[k])):
            if verbose:
                pidprint("Sheet:{}, \"Tid\" not found in first row".format(k))
            if 'Tid' in df[k].iat[0, 0]:
                real_names = [x for x in df[k].values[0].tolist() if isinstance(x, str) or (isinstance(x, float) and not np.isnan(x))]
                df[k] = pd.DataFrame(data=df[k].values[1:, :len(real_names)], columns=real_names)
                df[k]["Tid"] = pd.to_datetime(df[k]["Tid "])
                if verbose:
                    pidprint("Sheet:{}, Reformat successful".format(k))
            else:
                if verbose:
                    pidprint("Sheet:{}, Cannot Reformat. Leave as is.".format(k))
    return df


def read_CL(df_, verbose=False):

    if verbose:
        pidprint("Parsing...")

    df = {k: df_[k] for k in ["error", "Timkontroll", "LAB", "RESP", "CIRK"] if k in df_.keys()}
    df = check_df(df)

    #   Replace the 'Negativ' in Uringlukos by 0
    if 'Uringlukos' in list(df["LAB"]):
        df["LAB"]['Uringlukos'] = df["LAB"]['Uringlukos'].replace(to_replace='Negativ', value=0)
        df["LAB"]['Uringlukos'] = df["LAB"]['Uringlukos'].replace(to_replace='4++', value=4)
        df["LAB"]['Uringlukos'] = df["LAB"]['Uringlukos'].replace(to_replace='Spår', value=0.5)

    # Format variable names (remove trailing or heading spaces )
    for k in list(df):
        # df[k].set_index('Tid', inplace=True)
        df[k].rename(columns=lambda x:  "_".join([k, x.strip()]) if x.strip() != "Tid" else x.strip(), inplace=True)
        df[k].set_index('Tid', inplace=True)
        df[k].drop(columns=["_".join([k, "Variabel"])], axis=1, inplace=True)

    all_meds = read_injection(df_, sheet_name="LM_Givet")
    all_fluids = read_injection(df_, sheet_name='Vätska_givet')
    # Load medicines which are in a different layout

    # Concatenate all data frames
    dout = pd.concat([df[k] for k in list(df)], axis=0, sort=False).sort_index()
    dout.rename(columns=lambda x: format_str(x), inplace=True)

    for x in [k for k in ["resp_leoniplus", "resp_fabian_mode","resp_servou_mode"] if k in list(dout)]:
        dout[x] = dout[x].fillna("")

    # Make sure that Lab values are lab values
    dout.rename(columns=lambda x: find_labvalues(x), inplace=True)
    return dout, all_meds, all_fluids


def find_labvalues(x):
    sheetname, varname = parse("{}_{}", x)

    if ("vb_" in varname) or ("tc_" in varname) or ("ab_" in varname) or ("a_ad" in varname):
        return "{}_{}".format("lab", varname)
    else:
        return x


def find_respirator(d):
    sel = lambda var: any([x in var for x in ["fabian", "leoniplus", "servo", "cpap_sipap"]])
    p_var = [s for s in list(d) if sel(s)]
    if len(p_var) > 0:
        # Fill in the NaNs,
        # sometimes there are numbers so make sur to convert everything as a str,
        # sum along the first axis,
        # replace all the empty strings with nans and drop them
        # sometimes several respirator are reported functioning at the same time,
        # This leads to names such as cpaphfo, ...
        # I replace these with the first one.

        tmp = d[p_var].fillna("")\
            .applymap(lambda s: format_str(str(s)))\
            .sum(1)\
            .apply(lambda s: s if s!="" else np.nan)\
            .dropna(how="all")\
            .replace(to_replace={"cpapsippv":"cpap", 'nasal__cpapstandby':'nasal__cpap', 'bilevelstandby':"bilevel",
                                 'nasal__cpaphfo':'nasal__cpap','nasal__cpapsippv':'nasal__cpap' })
        # Create a clean DF and remove the duplicated timestamps
        out = pd.DataFrame(columns=["respirator"], data=tmp.values, index=tmp.index)
        out = out[~out.index.duplicated()]
        out = out.reset_index().dropna().set_index('Tid')
        out = out[out["respirator"] != ""]
        out = out.resample("10T").bfill(20)
        out = out.fillna("unknown")
    else:
        # In case of the resp, assume unknown
        out = pd.DataFrame(columns=["respirator"], data=["unknown"] * d.index.shape[0], index=d.index)
    return out


def group_temp(d):
    d_ = d.applymap(lambda x: np.nan if isinstance(x, str) else float(x))
    return np.nanmean(d_.values, axis=1)[0]


def find_temp(d):
    sel = lambda var: any([x in var for x in ["axil", "temp__ora", "temp__kad"]])
    p_var = [s for s in list(d) if sel(s)]
    if len(p_var) > 0:
        pidprint("Found temp:", p_var)
        tmp = d[p_var].applymap(lambda s: s.strip() if isinstance(s, str) else s).dropna(how="all").groupby(level=0).apply(group_temp)
        out = pd.DataFrame(columns=["cirk_tempaxil"], data=tmp.values, index=tmp.index).dropna(how="all")
        out = resample(out)

    else:
        out = pd.DataFrame(columns=["cirk_tempaxil"])
    return out


def gather_temp(d, fname_out, clinid):
    try:
        out = find_temp(d)

    except:
        out = pd.DataFrame(columns=["cirk_tempaxil"])

    write_df(append_clinid(out, clinid), format_fname(fname_out, "tempaxil"))


def gather_respi(d, fname_out, clinid):
    try:
        out = find_respirator(d)
    except:
        out = pd.DataFrame(columns=["respirator"], data=["standby"] * d.index.shape[0], index=d.index)
    write_df(append_clinid(out, clinid), format_fname(fname_out, "respirator"))


def find_ppeak(d, p_var):
    # If ppeak_u is found, we use that.
    if any(["ppeak_u" in x for x in p_var]):
        p_var_ = ["resp_ppeak_u_"]
        tmp = d[p_var_]
        ppeak = pd.DataFrame(columns=["ppeak"], data=tmp.values, index=tmp.index).dropna(how="all")

    elif any(["resp_pinsp_pman_i" in x for x in p_var]):
        tmp = d["resp_pinsp_pman_i"].dropna(how="all")
        ppeak = pd.DataFrame(columns=["ppeak"], data=tmp.values, index=tmp.index)

    elif any(["resp_awppeak_m_" in x for x in p_var]):
        tmp = d["resp_awppeak_m_"].dropna(how="all")
        ppeak = pd.DataFrame(columns=["ppeak"], data=tmp.values, index=tmp.index)

    else:  # otherwise use the default   #atmosphere
        pidprint("Cannot find correct pressure value, Found:", p_var)
        ppeak = pd.DataFrame(columns=["ppeak"], data=[1] * d.index.shape[0], index=d.index)

    return ppeak


def find_peep(d, p_var):
    p_var_ = [s for s in p_var if any(["peep" in s, "luftv_tr_med" in s, "cpap" in s])]

    if len(p_var_) > 0:
        # If peep_u is found, we use that.
        if any(["resp_peep_u_" in x for x in p_var_]):
            tmp = d["resp_peep_u_"].dropna(how="all")
            peep = pd.DataFrame(columns=["peep"], data=tmp.values, index=tmp.index)

        elif any(["resp_luftv_tr_med" in x for x in p_var_]):
            tmp = d["resp_luftv_tr_med"].dropna(how="all")
            peep = pd.DataFrame(columns=["peep"], data=tmp.values, index=tmp.index)

        elif any(["resp_peep1_m_" in x for x in p_var_]):
            tmp = d["resp_peep1_m_"].dropna(how="all")
            peep = pd.DataFrame(columns=["peep"], data=tmp.values, index=tmp.index)

        else:  # otherwise use the default  #atmosphere
            peep = pd.DataFrame(columns=["peep"], data=[1] * d.index.shape[0], index=d.index)

    else:  # otherwise use the default  #atmosphere
        peep = pd.DataFrame(columns=["peep"], data=[1] * d.index.shape[0], index=d.index)

    return peep


def gather_pressure(d, fname_out, clinid):
    """Find pressure variables, format them and write to file"""
    # Any pressure information
    p_var = [s for s in list(d) if pressure_sel(s)]

    if len(p_var) > 0:
        ppeak = find_ppeak(d, p_var)
        ppeak = ppeak[~ppeak.index.duplicated(keep="first") & ~ppeak.index.isna()]
        peep = find_peep(d, p_var)
        peep = peep[~peep.index.duplicated(keep="first") & ~peep.index.isna()]

        out = pd.concat([ppeak, peep], axis=1, sort=True)
        out.fillna(1, inplace=True)

    else:  # Nothing was found at all, #atmosphere
        out = pd.DataFrame(columns=["ppeak", "peep"], data=[[1, 1]] * d.index.shape[0], index=d.index)

    write_df(append_clinid(out, clinid), format_fname(fname_out, "pressure"))


def write_df(d, fname):
    d.to_csv(fname, sep=";")


def gather_fio2(d, fname_out, clinid):
    """Find fio2 variables."""
    sel = lambda var: any([x in var for x in ["fio2", "fio2_"]])
    p_var = [s for s in list(d) if sel(s)]

    if len(p_var) > 0:
        if any(["resp_fio2_u_" in x for x in p_var]):
            p_var = ["resp_fio2_u_"]
            tmp = d[p_var].dropna(how="all")
            out = pd.DataFrame(columns=["fio2"], data=tmp.values, index=tmp.index).dropna(how="all")

        elif any(["timkontroll_fio2_" in x for x in p_var]):
            p_var = ["timkontroll_fio2_"]
            tmp = d[p_var].dropna(how="all")
            out = pd.DataFrame(columns=["fio2"], data=tmp.values, index=tmp.index).dropna(how="all")

        elif any(["resp_fio2_m_" in x for x in p_var]):
            p_var = ["resp_fio2_m_"]
            tmp = d[p_var].dropna(how="all")
            out = pd.DataFrame(columns=["fio2"], data=tmp.values, index=tmp.index).dropna(how="all")

        else: # atmosphere
            out = pd.DataFrame(columns=["fio2"], data=[21] * d.index.shape[0], index=d.index)

    else: # atmosphere
        out = pd.DataFrame(columns=["fio2"], data=[21] * d.index.shape[0], index=d.index)

    if not out.empty:
        write_df(append_clinid(out.groupby(level=0).mean(), clinid), format_fname(fname_out, "fio2"))
    else:
        write_df(append_clinid(out, clinid), format_fname(fname_out, "fio2"))


def gather_med(dread, fname_out, clinid):
    """Find all the medications from lm_givet sheet"""
    try:
        out = dread[[s for s in list(dread) if "lm_givet_" in s]].dropna(how='all').groupby(level=0).mean()
    except:
        out = create_empty(["lm_givet_empty"])
    append_clinid(out, clinid).to_csv(format_fname(fname_out, "med"), sep=";")


def gather_fluids(dread, fname_out, clinid):
    """Find all the medications from lm_givet sheet"""
    try:
        out = dread[[s for s in list(dread) if "vatska_givet_" in s]].dropna(how='all').groupby(level=0).mean()
    except:
        out = create_empty(["vatska_givet_empty"])
    append_clinid(out, clinid).to_csv(format_fname(fname_out, "vatska"), sep=";")


def gather_lab(dread, fname_out, clinid):
    """Find all the lab values from lab sheet"""
    try:
        out = dread[[s for s in list(dread) if "lab_" in s]].dropna(how='all').groupby(level=0).mean()

        # Sometimes the lab values are duplicated
        out = out.loc[:, ~out.columns.duplicated()]

    except:
        out = create_empty(["lab_empty"]) # pd.DataFrame()

    append_clinid(out, clinid).to_csv(format_fname(fname_out, "lab"), sep=";")

def resample(out):
    out = out[~out.index.duplicated()]
    out = out.reset_index().dropna().set_index('Tid')
    out = out[out["cirk_vikt"] != ""]
    out = out.resample("3H").mean()
    out["cirk_vikt"].interpolate("linear", inplace=True)
    return out

def gather_vikt(dread, fname_out, clinid):
    try:
        out = dread[[s for s in list(dread) if "vikt" in s]].dropna(how='all').groupby(level=0).mean()
        if clinid in fix_clin_data.keys():
            if "CIRK" in fix_clin_data[clinid].keys():
                if "Vikt" in fix_clin_data[clinid]["CIRK"].keys():
                    out=out.replace(fix_clin_data[clinid]["CIRK"]["Vikt"]).dropna()

        # Catch values written in g rather than kg
        out[out > 200] = out[out > 200] / 1000

        out = resample(out)
    except:
        out = create_empty(["cirk_vikt"])

    append_clinid(out, clinid).to_csv(format_fname(fname_out, "vikt"), sep=";")


def create_empty(columns):
    return pd.DataFrame(columns=["Tid"] + columns).set_index("Tid")


parser = argparse.ArgumentParser(description='Read a clinisoft file into csv')
parser.add_argument('-i', metavar='Input', type=str, nargs=1,
                    help='Input Clinisoft Excel file.')
parser.add_argument('-o', metavar='Output', type=str, nargs=1,
                    help='Out Clinisoft .csv file.')
args = parser.parse_args()


def find_pn(df_):
    df = df_["GeneralData"]
    df.rename(columns=lambda x: x.strip(), inplace=True)
    out = df["SocSecurity"].values[0]
    return out


def get_clinid(fname):
    return parse("{}.xlsx", os.path.basename(fname))[0]


def empty_idx():
    return pd.DataFrame(columns=["hpn", "clinid"])


if __name__ == "__main__":
    fname_in = args.i[0]
    fname_out = os.path.abspath(args.o[0])
    dir_out = os.path.dirname(fname_out)
    verbose = True

    if not os.path.isdir(dir_out):
        os.makedirs(dir_out)

    if verbose:
        pidprint("Reading", fname_in)
    try:
        sheets_to_load=["RESP","CIRK", "Timkontroll","LAB","LM_Givet", "Vätska_givet"]
        df = pd.read_excel(fname_in, sheet_name=sheets_to_load, na_values='-', parse_dates=True)
    except:
        sheets_to_load=["RESP","CIRK","LAB","LM_Givet", "Vätska_givet"]
        df = pd.read_excel(fname_in, sheet_name=sheets_to_load, na_values='-', parse_dates=True)
    f = init_hash_fun()


    if "GeneralData" in list(df):
        pn = find_pn(df)
        clinid = get_clinid(fname_in)
        tmp_pn_df = empty_idx()
        tmp_pn_df["hpn"] = [f(format_pn(pn.strip()))]
        tmp_pn_df["clinid"] = [clinid]
        tmp_pn_df.to_csv(format_fname(fname_out, "hpn"), sep=";", index=False)
        del tmp_pn_df

    if verbose:
        pidprint("Done.")

    dread, all_meds, all_fluids = read_CL(df, verbose=True)

    # dread = pkl.load(open("tmp.pkl", "rb"))
    clinid = parse("{:d}.xlsx", os.path.basename(fname_in))[0]

    fun_list = [partial(f, dread, fname_out, clinid) for f in [gather_temp, gather_respi, gather_pressure,
                                                               gather_fio2, gather_lab, gather_vikt]]

    # list(map(lambda x: x(), fun_list))
    #gather_lab(dread, fname_out, clinid)
    gather_med(all_meds, fname_out, clinid)
    gather_fluids(all_fluids, fname_out, clinid)

    with Pool(3) as pool:
        pool.map(lambda x: x(), fun_list)

