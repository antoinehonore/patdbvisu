
d = {
    "cps_los": "^tkevt__los/.*/culture__sam.*$",
     "cps_eos": "^tkevt__eos/.*/culture__sam.*$",
     "cns_eos": "^tkevt__cnsepsis/eos.*(5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22).*/culture__sam.*$",
     "cns_los": "^tkevt__cnsepsis/los.*(5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22).*/culture__sam.*$",
     "sro": "^tkevt__(sepsis__ruled__out|sro)/.*/culture__sam.*$",
     "pneumonia": "^tkevt__pneumonia/.*/culture__sam.*$",
     "cns_infection": "^tkevt__cns__inf/[^ro].*/culture__s.*$",
     "abdominal_nec": "^tkevt__abdominal/nec/.*$",
     "brain_ivh_stage_3_4": "^tkevt__brain/ivh/diag__stage__(3|4)$",
     "lung_bleeding": "^tkevt__crsystem/lung__bleeding/acute$",
     "no_event": "^tkevt__no__event/no__notes/no__notes$",
     "death": "^tkevt__death/death/death$"
     }


d["los"] = "({}|{})".format(d["cns_los"], d["cps_los"])
d["eos"] = "({}|{})".format(d["cns_eos"], d["cps_eos"])

d["cns"] = "({}|{})".format(d["cns_eos"], d["cns_los"])

d["sepsis"] = "(" + "|".join([d["cps_los"], d["cps_eos"], d["cns_los"], d["cns_eos"]]) + ")"


d["sepsis_wo_eos"] = "("+"|".join([d["cps_los"], d["cns_los"]]) + ")"


d["neo_adverse"] = "(" + "|".join([d["sepsis"],
                                   d["abdominal_nec"],
                                   d["brain_ivh_stage_3_4"],
                                   d["lung_bleeding"],
                                   d["pneumonia"],
                                   d["cns_infection"]
                                   ]) + ")"

d["bleeding"] = "(" + "|".join([d["brain_ivh_stage_3_4"],
                                d["lung_bleeding"]
                                ]) + ")"

d["infection"] = "(" + "|".join([d["sepsis"],
                                 d["cns_infection"],
                                 d["pneumonia"],
                                 d["abdominal_nec"]
                                 ]) + ")"
