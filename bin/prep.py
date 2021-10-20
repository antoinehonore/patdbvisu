

import argparse
import pandas as pd
from unidecode import unidecode

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str)
parser.add_argument("-o", type=str)


if __name__ == "__main__":
    args = parser.parse_args()
    infname = args.i
    outfname = args.o
    import re

    rx = re.compile('\W+')

    df = pd.read_excel(infname)
    df.columns = [rx.sub(' ', unidecode(s.strip())).strip().lower().replace(" ","_") for s in df.columns]
    df.replace(";", "", regex=True).to_csv(outfname, sep=";",index=False)
