
from utils import gdate, date_fmt, get_engine,pidprint, get_dbcfg

s="select 'select ids__interval from takecare where '||string_agg('\"'||column_name||'\" notnull',' or ') as thequery "\
    "from (SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'takecare'"\
    "and column_name ~ '{}') as f;"

d={"los":"^tkevt__los/.*/culture__sam.*$",
 "eos":"^tkevt__eos/.*/culture__sam.*$",
 "cns":"^tkevt__cnsepsis/(5|6|7|8|9|10|11|12|13).*/culture__sam.*$",
 "sro":"^tkevt__(sepsis__ruled__out|sro)/.*/culture__sam.*$",
 "infection":"^tkevt__(.*infection|pneumonia)/.*/culture__sam.*$",
"pneumonia":"^tkevt__pneumonia/.*/culture__sam.*$",
 "cns_infection":"^tkevt__cns__infection/.*/culture__sam.*$",
 "abdominal_nec":"^tkevt__abdominal/nec/.*$",
 "brain_ivh_stage_3_4":"^tkevt__brain/ivh/diagnosis__stage__(3|4)$",
 "lung_bleeding":"^tkevt__crsystem/lung__bleeding/acute$",
 "no_event":"^tkevt__no__event/no__note__on.*$",
 "death":"^tkevt__death/death/death$"}

queries = {k: s.format(v) for k, v in d.items()}
dbcfg = get_dbcfg("cfg/db.cfg")

engine = get_engine(verbose=False, **dbcfg)
import pandas as pd

if __name__ == "__main__":
    print(queries["los"])
    q_str = {}
    res={}
    for k, v in queries.items():
        with engine.connect() as con:
            s = pd.read_sql(v, con).loc[0, "thequery"]
        q_str[k] = s

    for k, v in q_str.items():
        with engine.connect() as con:
            s = pd.read_sql(v, con).loc[0, "ids__interval"]
        res[k]=s

    print("")