import argparse
from src.utils import get_engine, get_dbcfg, read_query_file
import pandas as pd
import sys

s1 = "select 'select ids__uid,ids__interval from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery " \
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}') as f;"

s2 = "select 'select distinct ids__uid from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery " \
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}') as f;"

d = {"cps_los": "^tkevt__los/.*/culture__sam.*$",
     "cps_eos": "^tkevt__eos/.*/culture__sam.*$",
     "cns_eos": "^tkevt__cnsepsis/eos.*(5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22).*/culture__sam.*$",
     "cns_los": "^tkevt__cnsepsis/los.*(5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22).*/culture__sam.*$",
     "sro": "^tkevt__(sepsis__ruled__out|sro)/.*/culture__sam.*$",
     "pneumonia": "^tkevt__pneumonia/.*/culture__sam.*$",
     "cns_infection": "^tkevt__cns__inf/[^ro].*/culture__s.*$",
     "abdominal_nec": "^tkevt__abdominal/nec/.*$",
     "brain_ivh_stage_3_4": "^tkevt__brain/ivh/diagnosis__stage__(3|4)$",
     "lung_bleeding": "^tkevt__crsystem/lung__bleeding/acute$",
     "no_event": "^tkevt__no__event/no__notes/no__notes$",
     "death": "^tkevt__death/death/death$"
     }

d["los"] = "({}|{})".format(d["cns_los"], d["cps_los"])
d["eos"] = "({}|{})".format(d["cns_eos"], d["cps_eos"])


d["cns"] = "({}|{})".format(d["cns_eos"], d["cns_los"])

d["sepsis"] = "("+"|".format(d["cps_los"], d["cps_eos"], d["cns_los"], d["cns_eos"]) + ")"


d["sepsis_wo_eos"] = "("+"|".format(d["cps_los"], d["cns_los"]) + ")"


d["neo_adverse"] = "(" + "|".join([d["sepsis"],
                                   d["abdominal_nec"],
                                   d["brain_ivh_stage_3_4"],
                                   d["lung_bleeding"],
                                   d["pneumonia"],
                                   d["cns_infection"]
                                   ]) + ")"

d["bleeding"] = "(" + "|".join([d["brain_ivh_stage_3_4"],
                                d["lung_bleeding"]
                                ]) + ")"

d["infection"] = "(" + "|".join([d["sepsis"],
                                 d["cns_infection"],
                                 d["pneumonia"],
                                 d["abdominal_nec"]
                                 ]) + ")"

s3 = "drop view if exists view__tkgrp_{};"
s4 = "drop view if exists view__tkgrp_uid_{};"

queries1 = {k: s1.format(v) for k, v in d.items()}
queries2 = {k: s2.format(v) for k, v in d.items()}

drop = {k: s3.format(k) for k in d.keys()}
drop_uid = {k: s4.format(k) for k in d.keys()}

patview_list_query = "SELECT distinct table_name FROM information_schema.columns "\
               "WHERE table_schema = 'public' AND table_name like 'patview_%%'"

dbcfg = get_dbcfg("cfg/db.cfg")

engine = get_engine(verbose=False, **dbcfg)

parser = argparse.ArgumentParser()
parser.add_argument("--drop", dest='drop', action='store_const',
                    const=True, default=False,
                    help='Only drop all views.')

parser.add_argument("--clean", dest='clean', action='store_const',
                    const=True, default=False,
                    help='Clean the patview__')

parser.add_argument("--advanced", dest='advanced', action='store_const',
                    const=True, default=False,
                    help='Define the project specific advance views')


if __name__ == "__main__":
    args = parser.parse_args()

    if args.drop or args.clean:
        with engine.connect() as con:
            patview_list = pd.read_sql(patview_list_query, con)["table_name"].values.tolist()

        if len(patview_list) > 0:
            engine.execute("\n".join(["drop view if exists {};".format(k) for k in patview_list]))

        if args.drop:
            overall_drop_query = read_query_file("queries/drop_overall_views.sql")
            advanced_drop_query = read_query_file("queries/drop_advanced_views.sql")
            main_drop_query = read_query_file("queries/drop_views.sql")

            engine.execute(overall_drop_query)
            engine.execute(advanced_drop_query)
            engine.execute(main_drop_query)

            for k, v in drop.items():
                engine.execute(v)
            for k, v in drop_uid.items():
                engine.execute(v)
        sys.exit(0)

    q1_str = {}
    q2_str = {}

    res1 = {}
    res2 = {}

    # One pass of queries to find the columns relevant for 1 event
    for (k1, v1), (k2, v2) in zip(queries1.items(), queries2.items()):
        with engine.connect() as con:
            tmp1 = pd.read_sql(v1, con).loc[0, "thequery"]
            tmp2 = pd.read_sql(v2, con).loc[0, "thequery"]
        q1_str[k1] = tmp1
        q2_str[k1] = tmp2

    # A second pass to query the ids__interval for the columns obtained before
    for (k1, v1), (k2, v2) in zip(q1_str.items(), q2_str.items()):
        assert(k1 == k2)
        engine.execute(drop_uid[k1])
        engine.execute(drop[k1])
        if not (v1 is None):
            engine.execute("create view view__tkgrp_{} as ({});".format(k1, v1))
        if not (v2 is None):
            engine.execute("create view view__tkgrp_uid_{} as ({});".format(k2, v2))

    REGISTERED_TK_EVENTS = ",\n".join([
    "case when (via.ids__interval in (select v.ids__interval from view__tkgrp_{} v)) then 1 else 0 end as \"{}\"".format(k, k) for k in queries1.keys()])

    REGISTERED_UID_TK_EVENTS = ",\n".join([
    "case when (vua.ids__uid in (select v.ids__uid from view__tkgrp_uid_{} v)) then 1 else 0 end as \"{}\"".format(k, k) for k in queries1.keys()])



    main_query = read_query_file("queries/set_views.sql")
    engine.execute(main_query)

    advanced_query = read_query_file("queries/set_advanced_views.sql")
    engine.execute(advanced_query)


    overall_query = read_query_file("queries/set_overall_views.sql")
    overall_query = overall_query.replace("$REGISTERED_TK_EVENTS$", REGISTERED_TK_EVENTS)
    overall_query = overall_query.replace("$REGISTERED_UID_TK_EVENTS$", REGISTERED_UID_TK_EVENTS)
    engine.execute(overall_query)
