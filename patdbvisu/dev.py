from src.utils import get_engine, get_dbcfg, get_colnames, all_data_tables

dbcfg = get_dbcfg("cfg/db.cfg")
engine = get_engine(verbose=False, **dbcfg)

if __name__ == "__main__":

