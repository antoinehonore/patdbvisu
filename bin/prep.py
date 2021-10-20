

import argparse
import pandas as pd
from unidecode import unidecode
import numpy as np
import re

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str)
parser.add_argument("-o", type=str)


if __name__ == "__main__":
    args = parser.parse_args()
    infname = args.i
    outfname = args.o


    rx = re.compile('\W+')

    df = pd.read_excel(infname)
    df.columns = [rx.sub(' ', unidecode(s.strip())).strip().lower().replace(" ", "_") for s in df.columns]
    df.replace(";", "", regex=True).replace({".": np.nan, "-": np.nan}).to_csv(outfname, sep=";", index=False)
