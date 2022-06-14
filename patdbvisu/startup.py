import dash

from utils_db.utils_db import get_engine, get_dbcfg
from src.utils import get_colnames, all_data_tables

app = dash.Dash(
    __name__,
    external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)
app.title = "Oh My DB !"
server = app.server

dbcfg = get_dbcfg("cfg/db.cfg")
engine = get_engine(verbose=False, **dbcfg)


# The tables column names
with engine.connect() as con:
    all_cols = {k: get_colnames(k, con) for k in all_data_tables}
