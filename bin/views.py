import argparse
from utils import get_engine, get_dbcfg, read_query_file
import pandas as pd
import sys

s1 = "select 'select ids__uid,ids__interval from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery " \
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}') as f;"

s2 = "select 'select distinct ids__uid from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery " \
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}') as f;"


d = {"los": "^tkevt__los/.*/culture__sam.*$",
     "eos": "^tkevt__eos/.*/culture__sam.*$",
     "cns": "^tkevt__cnsepsis/(5|6|7|8|9|10|11|12|13).*/culture__sam.*$",
     "sro": "^tkevt__(sepsis__ruled__out|sro)/.*/culture__sam.*$",
     "infection": "^tkevt__(.*infection|pneumonia)/.*/culture__sam.*$",
     "pneumonia": "^tkevt__pneumonia/.*/culture__sam.*$",
     "cns_infection": "^tkevt__cns__infection/.*/culture__sam.*$",
     "abdominal_nec": "^tkevt__abdominal/nec/.*$",
     "brain_ivh_stage_3_4": "^tkevt__brain/ivh/diagnosis__stage__(3|4)$",
     "lung_bleeding": "^tkevt__crsystem/lung__bleeding/acute$",
     "no_event": "^tkevt__no__event/no__notes/no__notes$",
     "death": "^tkevt__death/death/death$"}


s3 = "drop view if exists view__tkgrp_{};"
s4 = "drop view if exists view__tkgrp_uid_{};"


queries1 = {k: s1.format(v) for k, v in d.items()}
queries2 = {k: s2.format(v) for k, v in d.items()}

drop = {k: s3.format(k) for k in d.keys()}
drop_uid = {k: s4.format(k) for k in d.keys()}


dbcfg = get_dbcfg("cfg/db.cfg")

engine = get_engine(verbose=False, **dbcfg)

parser = argparse.ArgumentParser()
parser.add_argument("--drop", dest='drop', action='store_const',
                    const=True, default=False,
                    help='Only drop all views.')


if __name__ == "__main__":
    args = parser.parse_args()

    if args.drop:
        main_drop_query = read_query_file("queries/drop_views.sql")
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


    REGISTERED_TK_EVENTS=",\n".join([
    "case when (via.ids__interval in (select v.ids__interval from view__tkgrp_{} v)) then 1 else 0 end as \"{}\"".format(k,k) for k in queries1.keys()])


    REGISTERED_UID_TK_EVENTS=",\n".join([
    "case when (vua.ids__uid in (select v.ids__uid from view__tkgrp_uid_{} v)) then 1 else 0 end as \"{}\"".format(k, k) for k in queries1.keys()])


    main_query = read_query_file("queries/set_views.sql")
    main_query = main_query.replace("$REGISTERED_TK_EVENTS$", REGISTERED_TK_EVENTS)
    main_query = main_query.replace("$REGISTERED_UID_TK_EVENTS$", REGISTERED_UID_TK_EVENTS)

    engine.execute(main_query)

