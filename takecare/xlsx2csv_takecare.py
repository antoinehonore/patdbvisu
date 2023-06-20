import pandas as pd
import sys
import argparse
import os
from multiprocessing.dummy import Pool
from functools import partial
from parse import parse
import numpy as np
from functools import reduce
import re
import unittest
from utils import get_nomenclature_ref,to_lower

parser = argparse.ArgumentParser(description='Parse a takecare rawfile.\nWrites the error log to stderr and the '\
                                             'parsed data to stdout')
parser.add_argument('-i', metavar="filename", help='Input raw takecare data file', type=str, nargs=1, required=True)
parser.add_argument('-nom', metavar="filename", help='Input ground truth nomenclature file', nargs=1, type=str, required=True)


class TestCase_is_valid_entry(unittest.TestCase):
    def inittest(self):
        return [re.compile(x) for x in ["[0-9]* days", "days", "days, (here|there)"]]

    def test_1(self):
        ref = self.inittest()
        x = "7 days"
        self.assertTrue(is_valid_entry(x, ref=ref))

    def test_2(self):
        ref = self.inittest()
        x = "a days"
        self.assertFalse(is_valid_entry(x, ref=ref))

    def test_3(self):
        ref = self.inittest()
        x = "a days"
        self.assertFalse(is_valid_entry(x, ref=ref))

    def test_4(self):
        ref = self.inittest()
        x = "days, here"
        self.assertTrue(is_valid_entry(x, ref=ref))


def is_valid_entry(x, ref=None):
    """returns 1 if x is in the reference list."""
    # res = [re.match(x, r) for r in ref if not (re.match(x, r) is None)]
    return len([r for r in ref if r.match(x)]) > 0


def check_vs_nom(ref, data=None):
    """Finds rows where word entry do not belong to nomenclature.
    Returns only the faulty row with '? : ' appended to the faulty column.
    Note: This does not check the consistency of a row.
    """
    # is_valid_entry("clinical signs,fever", ref[1])
    out = [data.loc[[i]] for i, x in enumerate(data[ref[0]].values) if not is_valid_entry(x, ref[1])]
    if len(out) > 0:
        # Concatenate individual rows
        not_in_nom = pd.concat(out)
        # Add a keyword to point at error
        not_in_nom[[ref[0]]] = not_in_nom[[ref[0]]].applymap(lambda x: "? : " + str(x))
    else:
        not_in_nom = pd.DataFrame()
    return not_in_nom


def check_spelling(s, spellCheck=None):
    return s.lower().replace("clincial", "clinical")\
        .replace("reintubation", "re-intubation")\
        .replace("evnt", "event")\
        .replace("parents", "parent")\
        .replace("sucessful", "successful")\
        .replace("distension", "distention")\
        .replace("clnical", "clinical")\
        .replace("symtoms", "symptoms")\
        .replace("bio signs, crp increase", "bio signs")\
        .replace("days of antibiotics", "days with antibiotics")\
        .replace("days antibiotics", "days with antibiotics")\
        .replace("hopital", "hospital")\
        .replace("dianosis", "diagnosis")\
        .replace("tacycardia", "tachycardia")\
        .replace("takycardia", "tachycardia")\
        .replace("takypnea", "tachypnea")\
        .replace("no changes", "no change")


def find_index_in_nom(d, tot_ref):
    """This returns the row number of the event|specificity|notes triple in the nomenclature.
                   -1  if it is not found or found at several places."""
    out = reduce(np.intersect1d, (
        [i for i, p in enumerate(tot_ref[k].values) if p.match(d[k])] for k in list(tot_ref.keys()))
           ).tolist()
    if len(out) == 0:
        return -1
    elif len(out) == 1:
        return out[0]
    else:
        print("Found several times in the nomenclature:", d.to_string(), file=sys.stderr)
        print("locations:", out, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Read arguments
    args = parser.parse_args()
    infile = args.i[0]
    nomenclature_fname = args.nom[0]

    tmp = parse("{}_v{:d}_takecare.xlsx",os.path.basename(infile))
    if not (tmp is None):
        i = tmp[1]
        new_basename = os.path.basename(nomenclature_fname).replace(".xlsx","_v{}.xlsx".format(i))
        nomenclature_fname = os.path.join(os.path.dirname(nomenclature_fname), new_basename)

    ref, tot_ref, nom = get_nomenclature_ref(nomenclature_fname)

    # Read Data
    data = to_lower(pd.read_excel(infile))

    # Make sure that all the patientID are non empty
    empty_idx = (data["patientid"] == "").values
    patid = np.unique(data["patientid"].values)
    fname = os.path.basename(infile).lower()

    if "" in patid:
        patid_error = "|".join(["patientID", fname, "", "some empty patientid rows", "", "", "patientID"])
        print(patid_error, file=sys.stderr)

    if patid.shape[0] > 1:
        patid_error = "|".join(["patientID", fname, "", "non unique patientid", "", "", "patientID"])
        print(patid_error, file=sys.stderr)

    if patid.shape[0] == 1 and ((fname != (patid[0] + "_v2_takecare.xlsx")) and (fname != (patid[0] + "_takecare.xlsx")) and (not ("covid" in fname))):
        patid_error = "|".join(["patientID", fname, "", "patientID differs from file name","","","patientID"])
        print(patid_error, file=sys.stderr)

    # This is used to tolerate and fix common typos
    fspell = partial(check_spelling, spellCheck=lambda x: x)
    data[['event', 'specificities', "notes"]] = data[['event', 'specificities', "notes"]].applymap(fspell)

    error_columns = ["dummy", "filename", "patientid", "date", "event", "specificities", "notes", "dummy"]


    # Carefully parse/format dates in the datafile
    tmp_date = pd.to_datetime(data["date"], format="%Y-%m-%d %H:%M",errors="coerce")
    errors = data[tmp_date.isna()].copy()

    if errors.shape[0] > 0:
        errors["dummy"] = "date"
        errors["filename"] = fname
        errors["date"] = errors["date"].apply(lambda s: "? : "+s)
        errors[error_columns].to_csv(sys.stderr, sep="|", index=False, header=False)

    data["date"] = pd.to_datetime(tmp_date, format='%Y-%m-%d %H:%M:%S')
    data["extra"]=data["extra"].replace("", "empty")
    data["extra"]=data["extra"].apply(lambda s:re.subn('[\',.\"~]', '',s)[0])

    all_todrop = [s for s in list(data) if not s in ["patientid","date","event","specificities","notes","extra"]]
    data.drop(columns=all_todrop, inplace=True)

    # Define the typo check function
    f = partial(check_vs_nom, data=data)

    # Run the misspelling check
    errors = pd.concat(list(map(f, ref.items())))
    errors["dummy"] = "typo"
    errors["filename"] = fname
    # Write all errors to stderr
    if not errors.empty:
        errors[error_columns].to_csv(sys.stderr, sep="|", index=False, header=False)

    # Assign group
    nom_headers = list(nom)
    groups = [x for x in nom_headers if "group" in x]
    if len(groups) > 0:
        ref_group_idx = nom_headers.index(groups[0])
        new_nom_headers = nom_headers[:ref_group_idx+1] + \
                          ["group_"+str(i+1)+"__"+x
                           for i, x in enumerate(nom_headers[ref_group_idx + 1:])]
        nom.columns = new_nom_headers
        groups = [x for x in new_nom_headers if "group_" in x]
        
        validity = ["beginning", "end"]
        nom[groups + validity] = nom[groups + validity].fillna(0)
        nom[groups] = nom[groups].astype(bool)
        nom[validity] = nom[validity].applymap(lambda x: float(x))
        groups_var = pd.DataFrame(index=nom.index, columns=["group"], data=[","]*nom.shape[0])

        for k in groups:
            groups_var.values[nom[k].values] += parse("group_{}_{}", k)[0] + ","

        data_group = pd.DataFrame(index=data.index, columns=["group", "beginning", "end"], data=[["", 0, 0]]*data.shape[0])

        for i in range(data.shape[0]):
            inom = find_index_in_nom(data[["event", "specificities", "notes"]].loc[i], tot_ref)
            if inom > -1:
                data_group["group"].values[i] = groups_var.values[inom][0]
                data_group["beginning"].values[i] = nom[["beginning"]].values[inom, 0]
                data_group["end"].values[i] = nom[["end"]].values[inom, 0]

        data_tmp = pd.concat([data, data_group], axis=1)
        data_tmp["dummy"] = "category"

        errors = data_tmp[data_tmp["group"].values == ""]

        # Write all errors to stderr
        if not errors.empty:
            errors["filename"] = fname
            errors[error_columns].to_csv(sys.stderr, sep="|", index=False, header=False)
        var_to_write = ["patientid", "date", "event", "specificities", "notes", "group", "beginning", "end", "extra"]
    else:
        data_tmp = data
        var_to_write = ["patientid", "date", "event", "specificities", "notes", "extra"]
    data_tmp[var_to_write].to_csv(sys.stdout, sep=";", index=False)

    sys.exit(0)
