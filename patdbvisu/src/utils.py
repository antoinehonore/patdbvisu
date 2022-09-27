from datetime import datetime
import pandas as pd
from dash import dcc, html
from dash import dash_table as dt


# Overwrting some CSS default styles
style_tbl = dict(
    filter_action="native",
    sort_action="native",
    sort_mode="multi",
    style_data={'color': 'black', 'backgroundColor': 'white'},
    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(220, 220, 220)', }],
    style_header={'backgroundColor': 'rgb(210, 210, 210)', 'color': 'black', 'fontWeight': 'bold'}
)

thestyle = {"margin": "auto"}
nav_bar_style = {}

date_fmt = "%Y-%m-%d %H:%M:%S"



def gentbl(df, style_table=None):
    return dt.DataTable(id='table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **style_tbl, page_size=10, style_table=style_table)


def gentbl_raw(df, id="newtbl", **kwargs):
    return dt.DataTable(id=id,
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        **kwargs)


def create_completion_dropdown():
    all_labels = ["Takecare", "Clinisoft", "Monitor LF", "Monitor HF"]
    all_values = [k.lower().replace(" ", "") for k in all_labels]
    tmp = dcc.Dropdown(
        options=[{"label": l, "value": v} for l, v in zip(all_labels, all_values)],
        value=all_values,
        # labelStyle={"display": "inline-block"},
        id="completion-dropdown",
        multi=True,
        placeholder="Choose the data sources",
        style=dict(width="450px")
    )
    return tmp



def get_latest_update(id, **kwargs):
    return html.Div([html.H3("Latest update: "), html.Div([html.P("-"), html.P("-")], id=id)], **kwargs)




def get_update_status(start_):
    return [html.P(gdate()), html.P("({} ms)".format(round(((datetime.now() - start_).total_seconds()) * 1000, 1)))]




def get_pat_intervals(thepatid, con):
    return pd.read_sql("select ids__interval from view__interv_all where ids__uid like '{}'".format(thepatid),
                            con).values.reshape(-1).tolist()


def run_select_queries(select_queries, engine):
    data = {}
    with engine.connect() as con:
        for k, v in select_queries.items():
            df = pd.read_sql(v, con)
            non_empty_col = (df.notna().sum(0) != 0)
            data[k] = df[non_empty_col.index.values[non_empty_col.values].tolist()]
    return data
