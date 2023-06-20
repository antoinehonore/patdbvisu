from functools import partial
import unidecode
import pandas as pd
import re
import unittest
import numpy as np

class TestCase_format_drop_down_lists(unittest.TestCase):
    def test_1(self):
        l = pd.DataFrame.from_dict({"first": ["thePattern", "the second"]})
        self.assertEqual(format_drop_down_lists(l), {"first": "(thepattern|the second)"})

    def test_2(self):
        l = pd.DataFrame.from_dict({"first": [np.nan, 12345, "thePattern", "the second", np.nan]})
        self.assertEqual(format_drop_down_lists(l), {"first": "(thepattern|the second)"})

class TestCase_format_ref(unittest.TestCase):
    def initlists(self):
        return format_drop_down_lists(pd.DataFrame.from_dict({"first": ["thePattern", "the second"],
                                    "malformations": ["thefirst", "thesecond"],
                                    "Type of Microbe": ["microb1", "microb2"],
                                    "treatments": ["med1", "med2"]}))

    def test_1(self):
        dlist = self.initlists()
        self.assertEqual(format_ref("theref", lists=dlist), re.compile("^theref$"))
    def test_2(self):
        dlist = self.initlists()
        self.assertEqual(format_ref("# (ref or references)" ,lists=dlist), re.compile("^[0-9].* (ref|references)$"))

    def test_3(self):
        dlist = self.initlists()
        self.assertEqual(format_ref("the type of microbe", lists=dlist), re.compile("^the (microb1|microb2)$"))

    def test_4(self):
        dlist = self.initlists()
        self.assertEqual(format_ref("the [treatments]", lists=dlist), re.compile("^the (med1|med2)$"))

    def test_5(self):
        dlist = self.initlists()
        self.assertEqual(format_ref("diagnosis, [malformations] thefdsfa",
                                    lists=dlist), re.compile("^diagnosis, (thefirst|thesecond) thefdsfa$"))


def format_drop_down_lists(lists):
    """Format drop down lists as multiple regexp 'or' matches."""
    return {k: "(" + "|".join([x.lower().strip() for x in lists[k].values.tolist() if isinstance(x, str)]) + ")"
            for k in list(lists)}


def format_str(s):
    s_c = ''.join(e if e.isalnum() else '_' for e in s)
    return unidecode.unidecode(s_c).lower()

def make_tkevt_key(data):
    return data["event"] + "/" + data["specificities"] + "/" + data["notes"]


def format_tkevt_string(s):
    return s.strip().lower().replace(",", "")\
        .replace("(", "").replace(".", "")\
        .replace(")", "").replace(" ", "__")\
        .replace("culture__negative__sepsis", "CNSepsis") \
        .replace("cardiorespiratory__system", "CRSystem") \
        .replace("staphylococcus", "staph")\


def get_nomenclature_ref(fname):
    # Read Nomenclature - Sheet 1
    nom = pd.read_excel(fname)
    nom.rename(columns=lambda x: format_str(x), inplace=True)
    nom[['event', 'specificities']] = nom[['event', 'specificities']].fillna(method="ffill")
    nom[['event', 'specificities', "notes"]] = to_lower(nom[['event', 'specificities', "notes"]])

    # Read Nomenclature - Sheet 2
    drop_down_lists = pd.read_excel(fname, sheet_name="Sheet2")
    re_lists = format_drop_down_lists(drop_down_lists)
    format_ref_fun = partial(format_ref, lists=re_lists)
    ref = {k: list(map(format_ref_fun, nom[k].unique())) for k in ['event', 'specificities', "notes"]}
    tot_ref = pd.DataFrame({k: list(map(format_ref_fun, nom[k])) for k in ['event', 'specificities', "notes"]})
    return ref, tot_ref, nom


def format_ref(ref_reg, lists=None):
    """This creates the regexp to match to meet one of the nomenclature rows."""
    regexp_str = ref_reg.lower()\
        .replace("#", "[0-9].*")\
        .replace(" or ", "|")\

    all_list_names = list(lists.keys())

    if "malformations" in all_list_names:
        regexp_str = regexp_str.replace("diagnosis, [malformations]", "diagnosis, {}".format(lists["malformations"]))
    if "Type of Microbe" in all_list_names:
        regexp_str = regexp_str.replace("type of microbe", ".*{}.*".format(lists["Type of Microbe"]))
    if "treatments" in all_list_names:
        regexp_str = regexp_str.replace("[treatments]", lists["treatments"])
    if "organ" in all_list_names:
        regexp_str = regexp_str.replace("[organ]", lists["organ"])
    return re.compile(r"^" + regexp_str + "$")


def to_lower(d):
    """Lower case and strip every string, and all non-strings into strings (nan ->  \"\") """
    d = d.rename(columns=lambda x: x.lower().strip())
    d = d.fillna("")
    return d.applymap(lambda x: x.lower().strip() if isinstance(x, str) else str(x))
