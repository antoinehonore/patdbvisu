


import argparse
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str, nargs="+")
from parse import parse
import os


if __name__ == "__main__":
    args = parser.parse_args()
    allfiles = args.i
    username = "anthon"
    passwd = "1234"
    schema = "public"

    engine = create_engine('postgresql://{}:{}@127.0.0.1:5432/patdb'.format(username, passwd))
    D = []
    all_tables_str="select t.table_name from information_schema.tables t where t.table_schema = \'{}\' and t.table_type = \'BASE TABLE\'".format(schema)
    with engine.connect() as con:
        all_tables = pd.read_sql(all_tables_str, con=con).values.reshape(-1).tolist()

    for fname in allfiles:
        df = pd.read_csv(fname, sep=";")
        tbl_name = parse("{}.csv", os.path.basename(fname))[0]

        if tbl_name in all_tables:
            with engine.connect() as con:
                df_exising = pd.read_sql("select * from public.{}".format(tbl_name), con)
        else:
            table_creation_fname = "cfg/{}.cfg".format(tbl_name)
            with open(table_creation_fname, "r") as fp:
                table_create_stmt = fp.read()

            with engine.connect() as con:
                con.execute(table_create_stmt)

            with engine.connect() as con:
                df.to_sql(tbl_name, con=con, index=False, if_exists="append")


        print("")
