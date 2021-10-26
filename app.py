import plotly.graph_objects as go # or plotly.express as px
fig = go.Figure() # or any Plotly Express function e.g. px.bar(...)

import dash
from dash import Dash, dcc, html, Input, Output, State,callback
import dash_table as dt
import pandas as pd
from bin.utils import get_engine, get_dbcfg, date_fmt,gdate
import pandas as pd
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

dbcfg = get_dbcfg("cfg/db.cfg")

style_tbl=dict(
style_cell_conditional = [
                             {
                                 'if': {'column_id': c},
                                 'textAlign': 'left'
                             } for c in ['Date', 'Region']
                         ],
style_data = {
                 'color': 'black',
                 'backgroundColor': 'white'
             },
style_data_conditional = [
                             {
                                 'if': {'row_index': 'odd'},
                                 'backgroundColor': 'rgb(220, 220, 220)',
                             }
                         ],
style_header = {
    'backgroundColor': 'rgb(210, 210, 210)',
    'color': 'black',
    'fontWeight': 'bold'
}
)
engine = get_engine(verbose=False, **dbcfg)

app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"])
#external_stylesheets=[dbc.themes.BOOTSTRAP])


app.layout = html.Div([
                html.Div([
                    html.H1("- Oh My DB ! -", style={'text-align': 'center'}),
                    html.H4("Latest update"),
                    html.H5("-", id="update-status"),
                    html.Button('Refresh', id='show-secret')
                ]),
            html.Div(id='body-div'),
            ])



def gentbl(df):
    return dt.DataTable(id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **style_tbl, page_size=10, style_table={'overflowX': 'auto'})

def get_update_status(e):
    return "{} ({} ms)".format(gdate(), round(e*1000, 1))

from datetime import datetime

@app.callback(
    Output(component_id='body-div', component_property='children'),
    Output(component_id="update-status", component_property="children"),
    Input(component_id='show-secret', component_property='n_clicks')
)
def update_output(n_clicks):
    start_ = datetime.now()

    if n_clicks is None:
        raise PreventUpdate
    else:
        with engine.connect() as con:
            df = pd.read_sql("select * from overview;", con)
        out = gentbl(df)

        elapsed = (datetime.now() - start_).total_seconds()

        return out, get_update_status(elapsed)


if __name__ == "__main__":
    app.run_server(debug=False)
