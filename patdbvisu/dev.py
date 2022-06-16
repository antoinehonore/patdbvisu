
from utils_db.utils_db import get_engine, get_hf_data, get_dbcfg

if __name__ == "__main__":
    dbcfg = get_dbcfg("cfg/db.cfg")
    engine = get_engine(verbose=False, **dbcfg)
    intervs = ["db3765793e4fa989c5ccf2575888deec874fbbe3d86fbcc6be0129879a55fb15",
            "eabd26deb24719d8f637dcbac5825d553bbb4d72fb84f0508002a33bc8a103b9",
            "9b0cbee8f928f39fcf093a8524644194898987667198bf62810fd8eb5411ec33",
            "f991dbaabfdf1abf2a30a8fe7c705d12b52fb3e8c77791d921fc35cded11c622",
            "5c9ad41786729f980011a848ea919ede8b30efad7e8cd56e25e58ce98ba1d7d4",
            "f7526b4143ab3754fbd028f04f67954a8a931db8915f634c9b8e7e264e495a1a",
            "f868bfc2de846e1ac2db38da60896e7c1261e700d3f05f44f40781f46560423c",
            "c8160c91b7f97bd56296b529d847825821f4720389bab61510e61fe2353f3e37"]
    dfmon = get_hf_data(intervs, engine, verbose=2)
    print("")
