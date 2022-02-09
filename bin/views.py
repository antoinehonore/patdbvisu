import argparse
from src.utils import get_engine, get_dbcfg, read_query_file
from src.events import d
import pandas as pd
import sys

col_sel_="SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}'"


s1 = "select 'select ids__uid,ids__interval from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery " \
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}') as f;"

s2 = "select 'select distinct ids__uid from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery " \
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'" \
    "and column_name ~ '{}') as f;"

s3 = "drop view if exists view__tkgrp_{};"
s4 = "drop view if exists view__tkgrp_uid_{};"
s5 = "drop view if exists view__tkgrp_agg_{};"

queries1 = {k: s1.format(v) for k, v in d.items()}
queries2 = {k: s2.format(v) for k, v in d.items()}
col_sel = {k: col_sel_.format(v) for k, v in d.items()}

drop = {k: s3.format(k) for k in d.keys()}
drop_uid = {k: s4.format(k) for k in d.keys()}
drop_evt_agg = {k: s5.format(k) for k in col_sel.keys()}


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
            for k, v in drop_evt_agg.items():
                engine.execute(v)

        sys.exit(0)

    q1_str = {}
    q2_str = {}
    thecols = {}
    the_tkevt_agg_view={}
    res1 = {}
    res2 = {}

    # One pass of queries to find the columns relevant for 1 event
    for (k1, v1), (k2, v2), (k3, v3) in zip(queries1.items(), queries2.items(), col_sel.items()):
        with engine.connect() as con:
            tmp1 = pd.read_sql(v1, con).loc[0, "thequery"]
            tmp2 = pd.read_sql(v2, con).loc[0, "thequery"]
            tmp3 = pd.read_sql(v3, con).values.reshape(-1).tolist()

        q1_str[k1] = tmp1
        q2_str[k1] = tmp2
        thecols[k3] = tmp3
        the_tkevt_agg_view[k3] = "select ids__uid,ids__interval, concat_ws('___'," + ", ".join(
            list(map(lambda s: "\"" + s + "\"", thecols[k3]))) + ") as \"{}\" from takecare".format(k3)

    # A second pass to query the ids__interval for the columns obtained before
    for (k1, v1), (k2, v2), (k3, v3) in zip(q1_str.items(), q2_str.items(), the_tkevt_agg_view.items()):
        assert(k1 == k2)
        engine.execute(drop_uid[k1])
        engine.execute(drop[k1])
        engine.execute(drop_evt_agg[k3])

        if not (v1 is None):
            engine.execute("create view view__tkgrp_{} as ({});".format(k1, v1))
        if not (v2 is None):
            engine.execute("create view view__tkgrp_uid_{} as ({});".format(k2, v2))
        if not (v3 is None):
            engine.execute("create view view__tkgrp_agg_{} as ({});".format(k3, v3))

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
