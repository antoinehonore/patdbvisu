import plotly.graph_objects as go  # or plotly.express as px

fig = go.Figure()  # or any Plotly Express function e.g. px.bar(...)

import dash
from dash import Dash, dcc, html, Input, Output, State, callback
from dash import dash_table as dt

import pandas as pd
from bin.utils import get_engine, get_dbcfg, date_fmt, gdate
import pandas as pd
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from datetime import datetime

all_data_tables = ["overview", "takecare", "med", "vatska", "vikt", "respirator", "pressure", "lab", "fio2", "monitorlf", "monitorhf"]


def pat_data_q(tbl_name, ids__uid, col="*"):
    return "select {} from {} where ids__uid=\'{}\'".format(col, tbl_name, ids__uid)


def get_db_size():
    with engine.connect() as con:
        d = pd.read_sql("select pg_size_pretty(pg_database_size(\'patdb\'))", con)
    return d.loc[0, "pg_size_pretty"]


def gentbl(df):
    return dt.DataTable(id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **style_tbl, page_size=10, style_table={'overflowX': 'auto'})


def get_update_status(start_):
    return [gdate(),html.Br(),"({} ms)".format(round(((datetime.now() - start_).total_seconds()) * 1000, 1))]


def cond_from_checklist(value):
    return " and ".join([v + "=1" for v in value])


def get_categories():
    with engine.connect() as con:
        l = pd.read_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'view__uid_has'",
            con)["column_name"].values.tolist()
    return [{"label": k, "value": k} for k in [ll for ll in l if ll != "ids__uid"]]


def new_checklist(i, init_val=None):
    return dcc.Dropdown(
        options=get_categories(),
        value=init_val,
        #labelStyle={"display": "inline-block"},
        id="checklist-{}".format(i),
        multi=True,
        placeholder="Write the categories you want",
        style=dict(width="1000px")
    )


dbcfg = get_dbcfg("cfg/db.cfg")

# Overwrting some CSS default styles
style_tbl = dict(
    filter_action="native",
    sort_action="native",
    sort_mode="multi",
    style_data={'color': 'black', 'backgroundColor': 'white'},
    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(220, 220, 220)', }],
    style_header={'backgroundColor': 'rgb(210, 210, 210)', 'color': 'black', 'fontWeight': 'bold'}
)

engine = get_engine(verbose=False, **dbcfg)

app = dash.Dash(
    __name__,
    external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"])

nav_bar_style = {"background-repeat": "no-repeat",
                 "background-position": "right top",
                 "background-size": "300px 30px",
                 "height": "5%",
                 "position": "fixed",
                 "top": "0", "width": "100%"}

nav_bar_style = {}

separator = html.Img(src=app.get_asset_url('line.png'), style={"width": "100%", "height": "5px"})

def get_latest_update(id="update-status"):
   return html.Div([html.P("Latest update: "), html.P("-", id=id)], className="two columns")


navbar = html.Div(children=[html.Ul(children=[html.Div([
    html.H1("- Oh My DB ! -", style={'text-align': 'center'}),
]),
    html.Div([
        html.Button('Refresh', id='refresh-button'),
    ], className="two columns"),
    get_latest_update(id="update-status"),
    html.Div([
        html.P("Database size: "),
        html.P("-", id="dbinfo")
    ]),
])],
    style=nav_bar_style)


def execquerey(s, engine, col=None):
    with engine.connect() as con:
        df = pd.read_sql(s, con)
    return df

@app.callback(
    Output(component_id='body-div', component_property='children'),
    Output(component_id="update-status", component_property="children"),
    Output(component_id="dbinfo", component_property="children"),
    Input(component_id='refresh-button', component_property='n_clicks')
)
def update_output(n_clicks):
    start_ = datetime.now()

    if n_clicks is None:
        raise PreventUpdate
    else:
        df = execquerey("select * from overview;", engine)
        out = gentbl(df)
    return out, get_update_status(start_), get_db_size()


@app.callback(
    Output(component_property="children", component_id="div-checklists"),
    Input(component_id='lesschecklist-button', component_property='n_clicks'),
    Input(component_id='morechecklist-button', component_property='n_clicks'),
    Input(component_property="children", component_id="div-checklists"),
)
def update_check_lists(clickless, clickmore, checklist):
    ctx = dash.callback_context

    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    init_val = []
    if button_id == "lesschecklist-button":
        return checklist[:-1]
    else:
        # print(button_id, clickless, clickmore, checklist[-1] if len(checklist) else checklist)

        if len(checklist) > 0:
            init_val = checklist[-1]["props"]["value"]
            pass
        return checklist + [new_checklist(len(checklist) + 1, init_val=init_val)]



@app.callback(
    Output("patientid-disp", "children"),
    Output("latest-update-patsearch", "children"),
    Input("patsearch-button", "n_clicks"),
    Input("input-patid", "value"),
)
def cb_render(n_clicks, patid):
    if n_clicks is None:
        raise PreventUpdate
    else:
        start_=datetime.now()
        d = {k: execquerey(pat_data_q(k, patid, col="ids__uid"), engine) for k in all_data_tables}
        return [html.P(", ".join([k for k, v in d.items() if v.shape[0] > 0]))], get_update_status(start_)


@app.callback(
    Output(component_id="checklist-test", component_property="children"),
    Input(component_id="updatechecklists-button", component_property='n_clicks'),
    Input(component_property="children", component_id="div-checklists"),
)
def update_checklist_test(n_clicks, checklists):
    if n_clicks is None:
        raise PreventUpdate
    else:
        OUT = []
        for v in checklists:
            if len(v["props"]["value"]) > 0:
                thequery = "select ids__uid from view__uid_has where {}".format(
                    cond_from_checklist(v["props"]["value"]))

                with engine.connect() as con:
                    dout = pd.read_sql(thequery, con).values.reshape(-1)
                OUT.append("\n".join(dout.tolist()))
        return "\n".join(OUT)




app.layout = html.Div([
    separator,
    navbar,
    separator,
    html.Div(id='body-div', style={"margin-top": "0px"}),
    html.H2("Population study", style={'text-align': 'left'}),
    html.Div([
        html.Button('More', id='morechecklist-button'),
        html.Button('Less', id='lesschecklist-button'),
        html.Button('Update', id='updatechecklists-button')
    ]),
    html.Div([
        html.Div(id="div-checklists", children=[]),
        html.P("This here", id="checklist-test")
    ]),
    separator,
    html.H2("Patient Display", style={'text-align': 'left'}),
    html.Div([
        dcc.Input(id="input-patid", placeholder="Write Patient ID"),
        html.Button('Search', id='patsearch-button'),
        get_latest_update(id="latest-update-patsearch"),
        html.Div(id="patientid-disp")
    ]),
    separator
])
import socket
if __name__ == "__main__":
    if socket.gethostname() == "cmm0576":
        app.run_server(debug=True)
    else:
        app.run_server(debug=False)