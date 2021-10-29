
from bin.utils import get_engine, read_query_file, get_dbcfg
import argparse
import os
import sys


parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str, help="Input query file name.",required=True)

if __name__ == "__main__":
    args = parser.parse_args()
    query_fname = args.i
    dbcfg = get_dbcfg("cfg/db.cfg")

    engine = get_engine(verbose=False, **dbcfg)
    if os.path.isfile(query_fname):
        query_str = read_query_file(query_fname,)
        exit_code = 0
    else:
        print(query_fname, "not found. Exit.", file=sys.stderr)
        exit_code = 1

    if not exit_code:
        engine = get_engine(verbose=False, **dbcfg)

        with engine.connect() as con:
            # print(query_str)
            rs = con.execute(query_str)

    sys.exit(exit_code)
