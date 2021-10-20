


import argparse
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str, nargs="+")
from parse import parse
import os

ov_col=['no', "ids__uid",'clinid', 'tkid', 'dsid', 'sex', 'birthdate', 'bw',
       'ga', 'ga_w', 'recruited_by', 'takecare_harvest', 'takecare_review',
       'unnamed_13', 'admission_cause', 'reserve_number', 'apgar_1', 'apgar_5',
       'apgar_10', 'delivery', 'head_circum', 'ph_umbilical_chord_a',
       'ph_umbilical_chord_v', 'base_excess', 'resucitation', 'betapred',
       'rupture_of_membrane', 'h_rom_bf_p', 'mother_ab_bef_p', 'ab', 'gbs',
       'chorioamnionitis', 'adrenaline', 'surfactant', 'caffeine',
       'medication_before_stud', 'crp_before', 'other', 'born_in', 'projid']


def get_primary_keys(tbl_name, engine):

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
        thekeys=pd.read_sql(query_s, con=con)
    return thekeys["column_name"].values.tolist()


def get_tables(schema,engine):
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


import numpy as np
import pickle as pkl


def read_csv(fname, thetypes):
    df = pd.read_csv(fname, sep=";")
    if not (thetypes is None):
        for c in df.columns:
            if thetypes[c].startswith("datetime64"):
                df[c] = pd.to_datetime(df[c])
            else:
                df[c] = df[c].astype(thetypes[c])
    return df


def fmt_sqldtype(x):
    if isinstance(x, str):
        out = x
    elif isinstance(x, pd.Timestamp):
        out = x.strftime("%Y-%m-%d %H:%M:%S")
    else:
        out = str(x)

    if not any([s == out for s in ["-", "nan", ".","NULL","None"]]):
        return "\'{}\'".format(out)
    else:
        return "NULL"


if __name__ == "__main__":
    args = parser.parse_args()
    allfiles = args.i
    username = "anthon"
    passwd = "1234"
    schema = "public"

    engine = create_engine('postgresql://{}:{}@127.0.0.1:5432/patdb'.format(username, passwd))
    D = []

    all_tables = get_tables(schema, engine)
    LOG={}
    for fname in allfiles:
        LOG[fname] = []
        tbl_name = parse("{}{:d}.csv", os.path.basename(fname))[0]

        if tbl_name in all_tables:
            thetypes = read_types(tbl_name)

            thekeys = get_primary_keys(tbl_name, engine)

            with engine.connect() as con:
                df_existing = pd.read_sql("select * from public.{}".format(tbl_name), con)#.set_index(thekeys)

            df = read_csv(fname, thetypes) #.set_index(thekeys)

            col = ",".join(df.columns)

            # Iterate on the new rows to upload
            for i in range(df.shape[0]):
                thekeyvalue = df.loc[i, thekeys[0]]

                # Row2dict
                row = {k: fmt_sqldtype(v) for k, v in df.iloc[i].to_dict().items()}

                # Find corresponding row based on primary key
                drow = df_existing[(df_existing[thekeys[0]] == thekeyvalue)]

                # Existing row -> dict
                if drow.shape[0] > 0:
                    row_exist = {k: fmt_sqldtype(v) for k, v in drow.iloc[0].to_dict().items()}
                else:
                    row_exist = {}

                if len(row_exist) == 0:  # Insert
                    to_update = row
                    with engine.connect() as con:
                        query_s = "insert into {}({}) values ({})".format(tbl_name,
                                                                          ",".join(to_update.keys()),
                                                                          ",".join(to_update.values()))
                        con.execute(query_s)
                        LOG[fname].append(query_s)

                else:  # update
                    to_update = {k: v for k, v in row.items() if v != row_exist[k]}
                    if len(to_update) > 0:
                        with engine.connect() as con:
                            the_update = ",".join(["{}={}".format(k, v) for k, v in to_update.items()])
                            query_s = "update {} set {} where {}={}".format(tbl_name,
                                                                            the_update,
                                                                            thekeys[0],
                                                                            fmt_sqldtype(thekeyvalue))
                            con.execute(query_s)
                            LOG[fname].append(query_s)

        else:
            table_creation_fname = "cfg/{}.cfg".format(tbl_name)
            with open(table_creation_fname, "r") as fp:
                table_create_stmt = fp.read()

            with engine.connect() as con:
                con.execute(table_create_stmt)

            the_data = pd.read_sql(tbl_name, engine)
            the_data.dtypes.to_pickle("cfg/{}.types".format(tbl_name))
            thetypes = read_types(tbl_name)

            df = read_csv(fname, thetypes)

            with engine.connect() as con:
                df.to_sql(tbl_name,
                          con=con,
                          index=False,
                          if_exists="append")
