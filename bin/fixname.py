from utils import mon_sig_name_fix
import sys, os

if __name__ == "__main__":
    s = sys.argv[1]
    sb, sext = s.split(".")
    print(mon_sig_name_fix(s))