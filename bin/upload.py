import argparse
import pandas as pd

from parse import parse
import os
from utils import gdate, date_fmt, get_engine



def get_primary_keys(tbl_name, engine, thedefault="ids__interval"):
    query_s="SELECT c.column_name, c.data_type FROM information_schema.table_constraints tc \
    JOIN information_schema.constraint_column_usage AS ccu \
    USING(constraint_schema, constraint_name) \
    JOIN \
    information_schema.columns AS c \
    ON c.table_schema = tc.constraint_schema \
    AND tc.table_name = c.table_name \
    AND ccu.column_name = c.column_name \
    WHERE constraint_type = \'PRIMARY KEY\' and tc.table_name = \'{}\'".format(tbl_name)
    with engine.connect() as con:
        thekeys = pd.read_sql(query_s, con=con)
    out=thekeys["column_name"].values.tolist()
    if len(out) == 0:
        out = [thedefault]
    return out


def get_tables(schema, engine):
    all_tables_str = "select t.table_name from information_schema.tables t where t.table_schema = \'{}\' and t.table_type = \'BASE TABLE\'".format(schema)
    with engine.connect() as con:
        all_tables = pd.read_sql(all_tables_str, con=con).values.reshape(-1).tolist()
    return all_tables


def clean_df_db_dups(df, tablename, engine, dup_cols=[],
                         filter_continuous_col=None, filter_categorical_col=None):
    """
    Remove rows from a dataframe that already exist in a database
    Required:
        df : dataframe to remove duplicate rows from
        engine: SQLAlchemy engine object
        tablename: tablename to check duplicates in
        dup_cols: list or tuple of column names to check for duplicate row values
    Optional:
        filter_continuous_col: the name of the continuous data column for BETWEEEN min/max filter
                               can be either a datetime, int, or float data type
                               useful for restricting the database table size to check
        filter_categorical_col : the name of the categorical data column for Where = value check
                                 Creates an "IN ()" check on the unique values in this column
    Returns
        Unique list of values from dataframe compared to database table
    """
    args = 'SELECT %s FROM %s' %(', '.join(['"{0}"'.format(col) for col in dup_cols]), tablename)
    args_contin_filter, args_cat_filter = None, None
    if filter_continuous_col is not None:
        if df[filter_continuous_col].dtype == 'datetime64[ns]':
            args_contin_filter = """ "%s" BETWEEN Convert(datetime, '%s')
                                          AND Convert(datetime, '%s')""" %(filter_continuous_col,
                              df[filter_continuous_col].min(), df[filter_continuous_col].max())


    if filter_categorical_col is not None:
        args_cat_filter = ' "%s" in(%s)' %(filter_categorical_col,
                          ', '.join(["'{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    dfdb = pd.read_sql(args, engine)
    df = pd.merge(df, dfdb, how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return df


def read_types(tbl_name):
    the_data = pd.read_pickle("cfg/{}.types".format(tbl_name))

    thetypes = {k: v.name for k, v in
                zip(the_data.index, the_data.values)}
    return thetypes


def read_csv(fname, thetypes):
    df = pd.read_csv(fname, sep=";")
    if not (thetypes is None):
        for c in df.columns:
            if c in thetypes:
                if thetypes[c].startswith("datetime64"):
                    df[c] = pd.to_datetime(df[c])
                elif thetypes[c].startswith("int"):
                    df[c] = df[c].fillna(-99999).astype(thetypes[c])
                else:
                    df[c] = df[c].astype(thetypes[c])
            else:  # assume str
                pass
    return df


def add_columns(df, schema, tbl_name, engine):
    list_cols_query = "SELECT column_name FROM information_schema.columns " \
                      "WHERE table_schema = \'{}\' " \
                      "AND table_name   = \'{}\';".format(schema, tbl_name)

    with engine.connect() as con:
        all_cols = pd.read_sql(list_cols_query, con).values.reshape(-1).tolist()
    missing_cols = [s for s in df.columns if not (s in all_cols)]
    if len(missing_cols) > 0:
        add_cols_query = "ALTER TABLE {} ".format(tbl_name) + ",".join(
            ["add column {} varchar".format("\"" + c.replace("%","%%") + "\"") for c in missing_cols])
        with engine.connect() as con:
            con.execute(add_cols_query)
        print(gdate(), "add columns", add_cols_query, file=sys.stderr)


def fmt_sqldtype(x):
    if isinstance(x, str):
        out = x
    elif isinstance(x, pd.Timestamp):
        out = x.strftime(date_fmt)
    else:
        out = str(x)

    if not any([s == out for s in ["-", "nan", ".","NULL","None"]]):
        return "\'{}\'".format(out)
    else:
        return "NULL"


from datetime import datetime
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str, nargs="+")
parser.add_argument("-nodup", type=int, default=1)

import json
def get_dbcfg(fname):
    with open(fname,"rb") as fp:
        dbcfg=json.load(fp)
    return dbcfg

if __name__ == "__main__":
    args = parser.parse_args()
    allfiles = args.i
    nodup = args.nodup

    LOG = {}
    dbcfg = get_dbcfg("cfg/db.cfg")

    engine = get_engine(verbose=True, **dbcfg)
    schema = dbcfg["schema"]

    D = []

    all_tables = get_tables(schema, engine)

    for fname in allfiles:
        LOG[fname] = []
        bname = os.path.basename(fname)

        # Infer table name from file
        stage1 = parse("{}_takecare.csv", bname)
        stage2 = parse("HF__{}.csv", bname)
        stage3 = parse("LF__{}.csv", bname)
        stage4 = parse("{}_read_{}.csv", bname)
        stage5 = parse("{}{:d}.csv", bname)
        stage6 = parse("{}.csv", bname)

        if stage1:
            tbl_name = "takecare"
        elif stage2:
            tbl_name = "monitorhf"
        elif stage3:
            tbl_name = "monitorlf"
        elif stage4:
            tbl_name = stage4[1]
        elif stage5:
            tbl_name = stage5[0]
        elif stage6:
            tbl_name = stage6[0]
        else:
            print("Could not infer tbl_name for", bname, file=sys.stderr)
            sys.exit(1)

        cfg_stem = tbl_name

        engine = get_engine(root_folder="/opt/psql", username="remotedbuser")

        # If the table exists
        if tbl_name in all_tables:
            # Find the types
            thetypes = read_types(tbl_name)

            # Find the keys, (right now ids__uid or ids__interval)
            thekeys = get_primary_keys(tbl_name, engine)

            # Read data
            df = read_csv(fname, thetypes)

            # Find duplicated indexes
            duplicated = df[thekeys[0]][df[thekeys[0]].duplicated()]

            if nodup & (duplicated.shape[0] > 0):
                print(gdate(), fname, "error", "duplicated IDs:\n{}".format(duplicated), file=sys.stderr)
                sys.exit(1)
            else:
                # Add a column if the existing db is missing one
                add_columns(df, schema, tbl_name, engine)

                col = ",".join(df.columns)

                # Iterate on the new rows to upload
                for i in range(df.shape[0]):
                    thekeyvalue = df.loc[i, thekeys[0]]

                    # Download the existing data
                    with engine.connect() as con:
                        drow = pd.read_sql("select * from {}.{} where {} like \'%%{}%%\'".format(schema,
                                                                                                 tbl_name,
                                                                                                 thekeys[0],
                                                                                                 thekeyvalue),
                                           con)

                    # Row2dict
                    row = {k: fmt_sqldtype(v) for k, v in df.iloc[i].to_dict().items()}

                    # Find corresponding row based on primary key
                    #drow = df_existing[(df_existing[thekeys[0]] == thekeyvalue)]

                    # Existing row -> dict
                    if drow.shape[0] > 0:
                        row_exist = {k: fmt_sqldtype(v) for k, v in drow.iloc[0].to_dict().items()}
                    else:
                        row_exist = {}

                    if len(row_exist) == 0:  # Insert
                        to_update = row
                        thevalues=",".join(to_update.values())
                        query_s = "insert into {}({}) values ({})".format(tbl_name,
                                                                          ",".join(list(
                                                                              map(lambda s: "\"{}\"".format(s),
                                                                                  to_update.keys()))),
                                                                          thevalues)
                        with engine.connect() as con:
                            con.execute(query_s.replace("%", "%%"))

                        infoprint = query_s if len(query_s) < 1000 else query_s.replace(thevalues,
                                                                                        "****************<Too long>****************")
                        print(gdate(), fname, "update", infoprint, file=sys.stderr)

                    else:  # update
                        to_update = {k: v for k, v in row.items() if v != row_exist[k]}
                        if len(to_update) > 0:
                            with engine.connect() as con:
                                the_update = ",".join(["\"{}\"={}".format(k, v) for k, v in to_update.items()])
                                query_s = "update {} set {} where {}={}".format(tbl_name,
                                                                                the_update,
                                                                                thekeys[0],
                                                                                fmt_sqldtype(thekeyvalue))
                                con.execute(query_s)
                                infoprint = query_s if len(query_s) < 1000 else query_s.replace(the_update,"****************<Too long>****************")
                                print(gdate(), fname, "update", infoprint, file=sys.stderr)

        else:
            table_creation_fname = "cfg/{}.cfg".format(cfg_stem)

            with open(table_creation_fname, "r") as fp:
                table_create_stmt = fp.read()

            with engine.connect() as con:
                con.execute(table_create_stmt)

            the_data = pd.read_sql(tbl_name, engine)
            the_data.dtypes.to_pickle("cfg/{}.types".format(tbl_name))
            thetypes = read_types(tbl_name)

            df = read_csv(fname, thetypes)

            add_columns(df, schema, tbl_name, engine)
            with engine.connect() as con:
                df.to_sql(tbl_name,
                          con=con,
                          index=False,
                          if_exists="append")
