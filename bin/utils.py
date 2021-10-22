from datetime import datetime

date_fmt="%Y-%m-%d %H:%M:%S"


def gdate(date_fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.now().strftime(date_fmt)
