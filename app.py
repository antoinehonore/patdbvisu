import plotly.graph_objects as go  # or plotly.express as px
import plotly.express as px

import dash
from dash import Dash, dcc, html, Input, Output, State, callback
from dash import dash_table as dt

import pandas as pd
from bin.utils import get_engine, get_dbcfg, date_fmt, gdate, all_data_tables, get_colnames,ref_cols,run_select_queries
import pandas as pd
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from datetime import datetime


def pat_data_q(tbl_name, ids__uid, col="*"):
    return "select {} from {} where ids__uid=\'{}\'".format(col, tbl_name, ids__uid)


def get_db_size():
    with engine.connect() as con:
        d = pd.read_sql("select pg_size_pretty(pg_database_size(\'patdb\'))", con)
    return d.loc[0, "pg_size_pretty"]


def get_db_npat():
    with engine.connect() as con:
        d = pd.read_sql("select count(distinct ids__uid) as \"ids__uid\" from view__uid_all", con)
    return "{} patients".format(d.loc[0, "ids__uid"])


def gentbl(df,style_table={'overflowX': 'auto'}):
    return dt.DataTable(id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **style_tbl, page_size=10, style_table=style_table)

def gentbl_raw(df, id="newtbl",**kwargs):
    return dt.DataTable(id=id,
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **kwargs)



def get_update_status(start_):
    return [gdate(), html.Br(), "({} ms)".format(round(((datetime.now() - start_).total_seconds()) * 1000, 1))]


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
        placeholder="Population {}: Write the categories you want".format(i),
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
    external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)

nav_bar_style = {"background-repeat": "no-repeat",
                 "background-position": "right top",
                 "background-size": "300px 30px",
                 "height": "5%",
                 "position": "fixed",
                 "top": "0", "width": "100%"}

nav_bar_style = {}

separator = html.Img(src=app.get_asset_url('line.png'), style={"width": "100%", "height": "5px"})

def get_latest_update(id,**kwargs):
   return html.Div([html.H3("Latest update: "), html.P("-", id=id)], **kwargs)


def fig_npat_vs_time(engine):
    with engine.connect() as con:
        df = pd.read_sql("select * from view__timeline_n_patients;", con).set_index("interval__start")

    df.rename(columns={
        k: k.replace("total_n_patients__", "").capitalize().replace("torlf", "tor LF").replace("torhf", "tor HF")
        for k in df.columns}, inplace=True)

    fig = px.line(df, template="none")

    fig.update_layout(font={"size": 30}, legend_title="Source")
    fig.update_traces(line=dict(width=5))

    fig.update_xaxes(title="")
    fig.update_yaxes(title="Number of Patients", automargin=True)
    return fig


def fig_pat_length_of_stay(engine):
    with engine.connect() as con:
        df = pd.read_sql("select n_days from view__length_of_stay where n_days <780;", con)

    fig = px.histogram(df, nbins=200, template="none")

    fig.update_layout(font={"size": 30}, showlegend=False)
    #fig.update_traces(line=dict(width=5))

    fig.update_yaxes(title="Patient Count", automargin=True)
    fig.update_xaxes(title="Length of stay (days)", automargin=True)
    return fig


def create_completion_dropdown():
    all_labels = ["Takecare", "Clinisoft", "Monitor LF", "Monitor HF"]
    all_values = [k.lower().replace(" ","") for k in all_labels]
    tmp = dcc.Dropdown(
        options=[{"label":l, "value":v} for l,v in zip(all_labels,all_values)],
        value=all_values,
        # labelStyle={"display": "inline-block"},
        id="completion-dropdown",
        multi=True,
        placeholder="Choose the data sources",
        style=dict(width="450px")
    )
    return tmp

cname=None
thestyle={'padding': 10, 'flex': 1}
thestyle={"margin":"auto"}
navbar = html.Div(children=[
            html.Div([
                html.H1("- Oh My DB ! -", style={'text-align': 'center'}),
            ]),
            html.Div([
                html.Button('Update', id='refresh-button', style=thestyle),
                get_latest_update("update-status", style=thestyle),
                html.Div([
                    html.H3("Database size: "),
                    html.P("-", id="dbinfo-size"),
                    html.P("-", id="dbinfo-npat")
                ], style=thestyle),
                html.Button('More', id='moreless-button',style=thestyle)
            ], style={'display': 'flex', 'flex-direction': 'row'}),
            html.Div([
                html.H2("Data overlap", style={'text-align': 'left'}),
                html.Div(id="div-db-completion", children=[create_completion_dropdown(),
                                                           html.P("-", id="completion-result")]),
                html.Div(id="div-db-details"),
            ])
        ],
    style=nav_bar_style)


@app.callback(
    Output("completion-result", "children"),
    Input("refresh-button", "n_clicks"),
    Input("completion-dropdown", "value")
)
def update_completion_data(n_clicks, dropdown):
    if n_clicks is None:
        raise PreventUpdate
    else:
        query_s = " intersect ".join(["select * from view__{}_has".format(k) for k in dropdown])+"\n order by ids__uid"
        query_s = "select count(distinct ids__uid) as \"Number of patients\", count(distinct ids__interval) as \"Number of intervals\" from (\n" + \
                  query_s +\
                  "\n) as foo;"
        with engine.connect() as con:
            intersection_data = pd.read_sql(query_s, con)
    return gentbl_raw(intersection_data,id="completion-count-tbl",style_table={"width":"450px"})

moreless = {"More": "Less", "Less": "More"}

@app.callback(
    Output(component_id="div-db-details", component_property="children"),
    Output(component_id="moreless-button", component_property="children"),
    Input(component_id="moreless-button", component_property="n_clicks"),
    Input(component_id='refresh-button', component_property='n_clicks'),
    Input(component_id="moreless-button", component_property="children")

)
def showhide_db_details(n_clicks, refresh_click,button_status):
    if n_clicks is None:
        raise PreventUpdate
    else:
        ctx = dash.callback_context

        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        out = []
        if (button_status == "More") or (button_id=="refresh-button" and button_status == "Less"):

            fig = fig_npat_vs_time(engine)
            out = [dcc.Graph(figure=fig, style={"margin-top": "50px"})]

            fig2 = fig_pat_length_of_stay(engine)
            out += [dcc.Graph(figure=fig2, style={"margin-top": "50px"})]

        new_status = moreless[button_status]
        return out, new_status

def execquerey(s, engine, col=None):
    with engine.connect() as con:
        df = pd.read_sql(s, con)
    return df

@app.callback(
    Output(component_id='body-div', component_property='children'),
    Output(component_id="update-status", component_property="children"),
    Output(component_id="dbinfo-size", component_property="children"),
    Output(component_id="dbinfo-npat", component_property="children"),
    Input(component_id='refresh-button', component_property='n_clicks')
)
def update_output(n_clicks):
    start_ = datetime.now()

    if n_clicks is None:
        raise PreventUpdate
    else:
        df = execquerey("select * from overview;", engine)
        out = gentbl(df)
    return out, get_update_status(start_), get_db_size(), get_db_npat()


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
        print(button_id)

        if len(checklist) > 0:
            init_val = checklist[-1]["props"]["value"]
            pass
        return checklist + [new_checklist(len(checklist) + 1, init_val=init_val)]


# The tables column names
with engine.connect() as con:
    all_cols = {k: get_colnames(k, con) for k in all_data_tables}

thecase = "case when ({} notnull) then True else NULL end as {}"

the_cases = {k: ",\n".join([col if (k == "overview") or (col in ref_cols) else thecase.format(col, col)
                           for col in all_cols[k]]) for k in all_data_tables}


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
        start_ = datetime.now()

        create_views_queries = {
            k: "create or replace view patview_{}_{} as (select * from {} where ids__uid='{}');".format(k, patid, k, patid) for k
            in all_data_tables}

        engine.execute("\n".join([v for v in create_views_queries.values()]))

        select_queries = {k: "select {} from patview_{}_{}".format(the_cases[k], k, patid) for k in all_data_tables}

        data_lvl1 = run_select_queries(select_queries, engine)

 #       drop_views_queries = {k: "drop view if exists patview_{}_{};".format(k, patid) for k in all_data_tables}


#        d = {k: execquerey(pat_data_q(k, patid, col="ids__uid"), engine) for k in all_data_tables}
        return [gentbl(data_lvl1["overview"])], get_update_status(start_)


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
        html.P("-", id="checklist-test")
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