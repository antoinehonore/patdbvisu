import plotly.graph_objects as go # or plotly.express as px
fig = go.Figure() # or any Plotly Express function e.g. px.bar(...)

import dash
from dash import Dash, dcc, html, Input, Output, State,callback
from dash import dash_table as dt

import pandas as pd
from bin.utils import get_engine, get_dbcfg, date_fmt, gdate
import pandas as pd
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from datetime import datetime


dbcfg = get_dbcfg("cfg/db.cfg")

# Overwrting some CSS default styles
style_tbl=dict(
    filter_action="native",
    sort_action="native",
    sort_mode="multi",
style_cell_conditional = [
            {'if': {'state': 'active'},
                'backgroundColor': 'rgba(0, 116, 217, 0.3)'
            },
            { 'if': { 'state': 'selected'},
            'backgroundColor': 'rgba(0, 116, 217, 0.3)'
            },
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
})


engine = get_engine(verbose=False, **dbcfg)

app = dash.Dash(
    __name__,
    external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"])

nav_bar_style = {"background-repeat": "no-repeat",
     "background-position": "right top",
     "background-size": "300px 30px",
     "height":"5%",
     "position":"fixed",
     "top":"0", "width": "100%"}

#nav_bar_style = {}
separator = html.Img(src=app.get_asset_url('line.png'), style={"width": "100%", "height": "5px"})

navbar = html.Div(children=[html.Ul(children=[html.Div([
                    separator,
                    html.H1("- Oh My DB ! -", style={'text-align': 'center'}),
                    ]),
                html.Div([
                    html.Button('Refresh', id='show-secret'),
                    ], className="two columns"),
                html.Div([
                    html.P("Latest update: "),
                    html.P("-", id="update-status")
                ], className="two columns"),
                html.Div([
                    html.P("Database size: "),
                    html.P("-", id="dbinfo")
                    ]),
                separator
                ])],
    style=nav_bar_style)


app.layout = html.Div([navbar,
             html.Div(id='body-div',style={"margin-top":"200px"}),
             ]+[html.P("dd")]*50)


def get_db_size():
    with engine.connect() as con:
        d = pd.read_sql("select pg_size_pretty(pg_database_size(\'patdb\'))",con)
    return d.loc[0, "pg_size_pretty"]


def gentbl(df):
    return dt.DataTable(id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                         **style_tbl, page_size=10, style_table={'overflowX': 'auto'})


def get_update_status(e):
    return "{} ({} ms)".format(gdate(), round(e*1000, 1))


@app.callback(
    Output(component_id='body-div', component_property='children'),
    Output(component_id="update-status", component_property="children"),
    Output(component_id="dbinfo", component_property="children"),
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

        return out, get_update_status(elapsed), get_db_size()


if __name__ == "__main__":
    app.run_server(debug=False)
