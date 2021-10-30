import argparse
from utils import get_engine, get_dbcfg, read_query_file, clin_tables,all_data_tables,run_select_queries
import pandas as pd
import sys

dbcfg = get_dbcfg("cfg/db.cfg")

engine = get_engine(verbose=False, **dbcfg)

parser = argparse.ArgumentParser()
parser.add_argument("--drop", dest='drop', action='store_const',
                    const=True, default=False,
                    help='Only drop all views.')

import plotly.graph_objects as go
import plotly.express as px
if __name__ == "__main__":
    args = parser.parse_args()

    transpose_q=\
    "  SELECT 'SELECT * FROM   crosstab(       $ct$SELECT u.attnum, t.rn, u.val         FROM  (SELECT row_number() OVER () AS rn, * FROM ' || attrelid::regclass || ') t "\
      "       , unnest(ARRAY[' || string_agg(quote_ident(attname) "\
     "                         || '::text', ',') || ']) "\
     "            WITH ORDINALITY u(val, attnum) "\
    "    ORDER  BY 1, 2$ct$ "\
   ") t (attnum bigint, ' "\
     "|| (SELECT string_agg('r'|| rn ||' text', ', ') "\
      "   FROM  (SELECT row_number() OVER () AS rn FROM tbl) t) "\
     "|| ')' AS sql "\
"FROM   pg_attribute "\
"WHERE  attrelid = 'tbl'::regclass "\
"AND    attnum > 0 AND    NOT attisdropped " \
"GROUP  BY attrelid;"

with engine.connect() as con:
    df = pd.read_sql(transpose_q,con)


out = engine.execute(df["sql"].iloc[0])


def get_colnames(k,con):
    df=pd.read_sql("select column_name   FROM information_schema.columns "\
                   " WHERE table_schema = \'public\'    AND table_name   = \'{}\' ".format(k),
                   con)
    return list(map(lambda s: "\""+s+"\"", df["column_name"].values.tolist()))
with engine.connect() as con:
    all_cols={k:get_colnames(k, con) for k in all_data_tables}
ref_cols = ["\""+s+"\"" for s in ['ids__uid', 'ids__interval', 'interval__raw', 'interval__start', 'interval__end']]

thecase = "case when ({} notnull) then True else NULL end as {}"

the_cases = {k:",\n".join([col if (k == "overview") or (col in ref_cols) else thecase.format(col, col)
                           for col in all_cols[k]]) for k in all_data_tables}


patid = "f44e1b73551b133630599a55037768ce87ead7e64d37ce1f3e906a6e21a7a5b1"


create_views_queries = {k: "create view patview_{}_{} as (select * from {} where ids__uid='{}');".format(k, patid, k, patid) for k in all_data_tables}

engine.execute("\n".join([v for v in create_views_queries.values()]))


select_queries = {k: "select {} from patview_{}_{}".format(the_cases[k], k, patid) for k in all_data_tables}


data_lvl1 = run_select_queries(select_queries, engine)

drop_views_queries = {k: "drop view if exists patview_{}_{};".format(k, patid) for k in all_data_tables}





engine.execute("\n".join([v for v in drop_views_queries.values()]))



