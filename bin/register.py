import argparse
import os
import subprocess
from utils import gdate, date_fmt, get_engine, pidprint, get_dbcfg,register_ids

def decrypt_file(fname):
    cmd = "gpg -d --passphrase-file=/opt/psql/gpg_antoine_pfile.txt --yes --batch {} 2>/dev/null".format(fname)
    s = subprocess.check_output(cmd, shell=True).decode("utf8")
    return s


def read_map(fname):
    if os.path.isfile(fname+".gpg"):
        c = decrypt_file(fname+".gpg").strip()
        out = {ll[0]: ll[1] for ll in [l.split(":") for l in c.split("\n")]}
    else:
        out = {}
    return out


import pandas as pd
parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str)

if __name__ == "__main__":
    args = parser.parse_args()
    fname = args.i
    themap = read_map(fname)

    register_ids(themap)

    print("")