from startup import app, engine
from src.utils import gentbl_raw, get_update_status
from datetime import datetime

import pandas as pd
import dash
from dash import dcc, Input, Output
from dash.exceptions import PreventUpdate

import plotly.express as px

def get_db_size():
    with engine.connect() as con:
        d = pd.read_sql("select pg_size_pretty(pg_database_size(\'patdb\'))", con)
    return d.loc[0, "pg_size_pretty"]


def get_db_npat():
    with engine.connect() as con:
        d = pd.read_sql("select count(distinct ids__uid) as \"ids__uid\" from view__uid_all", con)
    return "{} patients".format(d.loc[0, "ids__uid"])


@app.callback(
    Output(component_id="update-status", component_property="children"),
    Output(component_id="dbinfo-size", component_property="children"),
    Output(component_id="dbinfo-npat", component_property="children"),
    Input(component_id='refresh-button', component_property='n_clicks')
)
def update_output(n_clicks):
    start_ = datetime.now()
    if n_clicks is None:
        raise PreventUpdate
    return get_update_status(start_), get_db_size(), get_db_npat()

moreless = {"More": "Less", "Less": "More"}

@app.callback(
    Output("completion-result", "children"),
    Input("refresh-button", "n_clicks"),
    Input("completion-dropdown", "value")
)
def update_completion_data(n_clicks, dropdown):
    if n_clicks is None:
        raise PreventUpdate
    else:
        query_s = " intersect ".join(
            ["select * from view__{}_has".format(k) for k in dropdown]) + "\n order by ids__uid"
        query_s = "select count(distinct ids__uid) as \"Number of patients\"," \
                  "count(distinct ids__interval)::decimal/2 as \"Number of days\" from (\n" + \
                  query_s + "\n) as foo;"

        with engine.connect() as con:
            intersection_data = pd.read_sql(query_s, con)

    return gentbl_raw(intersection_data,
                      id="completion-count-tbl", style_table={"width": "450px"})



def fig_npat_vs_time(engine):
    with engine.connect() as con:
        df = pd.read_sql("select * from view__timeline_n_patients;", con).set_index("interval__start")

    df.rename(columns={
        k: k.replace("total_n_patients__", "").capitalize().replace("torlf", "tor LF").replace("torhf", "tor HF")
        for k in df.columns}, inplace=True)

    fig = px.scatter(df, template="none")
    fig.update_layout(font={"size": 30}, legend_title="Source")
    fig.update_traces(mode="markers+lines", line=dict(width=5), marker=dict(size=10))

    fig.update_xaxes(title="")
    fig.update_yaxes(title="Number of Patients", automargin=True)
    return fig


def fig_pat_length_of_stay(engine):
    with engine.connect() as con:
        df = pd.read_sql("select n_days from view__length_of_stay where n_days <780;", con)

    fig = px.histogram(df, nbins=200, template="none")

    fig.update_layout(font={"size": 30}, showlegend=False)
    # fig.update_traces(line=dict(width=5))

    fig.update_yaxes(title="Patient Count", automargin=True)
    fig.update_xaxes(title="Length of stay (days)", automargin=True)
    return fig


def fig_pat_unitname_overtime(engine):
    with engine.connect() as con:
        df_hf = pd.read_sql("select * from view__monitorhf_unitname;", con)
    with engine.connect() as con:
        df_lf = pd.read_sql("select * from view__monitorlf_unitname;", con)
    df = df_lf
    #df["unitname"] = df["unitname"].apply(lambda s: s.split("__")[0])
    tmphf = pd.get_dummies(df["unitname"])
    print(tmphf.columns)
    good_cols = [c for c in tmphf.columns if not ("__" in c)]

    dplot = pd.concat([df, tmphf[good_cols]], axis=1).drop(columns=["unitname"])
    dplot = dplot.groupby("interval__start").sum(0)
    dcols = [c for c in dplot.columns if c != "interval__start"]
    dplot[dcols] = dplot[dcols].cumsum(0)

    fig = px.scatter(dplot, y=dcols, template="none")
    fig.update_layout(font={"size": 30}, showlegend=True, legend_title="Ward")
    fig.update_yaxes(title="Patient Count", automargin=True)
    fig.update_xaxes(title="", automargin=True)

    fig.for_each_trace(lambda trace: trace.update(visible="legendonly") if trace.name in ["unknown"] else ())
    return fig

@app.callback(
    Output(component_id="div-db-details", component_property="children"),
    Output(component_id="moreless-button", component_property="children"),
    Input(component_id="moreless-button", component_property="n_clicks"),
    Input(component_id='refresh-button', component_property='n_clicks'),
    Input(component_id="moreless-button", component_property="children")
)
def showhide_db_details(n_clicks, refresh_click, button_status):
    if n_clicks is None:
        raise PreventUpdate
    else:
        ctx = dash.callback_context

        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        out = []
        if (button_status == "More") or (button_id == "refresh-button" and button_status == "Less"):
            fig = fig_npat_vs_time(engine)
            out = [dcc.Graph(figure=fig, style={"margin-top": "50px"})]

            fig2 = fig_pat_length_of_stay(engine)
            out += [dcc.Graph(figure=fig2, style={"margin-top": "50px"})]

            fig3 = fig_pat_unitname_overtime(engine)
            out += [dcc.Graph(figure=fig3, style={"margin-top": "50px"})]

        new_status = button_status
        if button_id == "moreless-button":
            new_status = moreless[button_status]

        return out, new_status
