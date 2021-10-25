import plotly.graph_objects as go # or plotly.express as px
fig = go.Figure() # or any Plotly Express function e.g. px.bar(...)

import dash
import dash_core_components as dcc
import dash_html_components as html
from bin.utils import get_engine, get_dbcfg
import pandas as pd

dbcfg = get_dbcfg("cfg/db.cfg")

engine = get_engine(verbose=False, **dbcfg)

with engine.connect() as con:
	df = pd.read_sql("select * from overview;",con)


def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])

app = dash.Dash()
app.layout = html.Div([
	html.H4(children='US Agriculture Exports (2011)'),
	generate_table(df),
	    dcc.Graph(figure=fig)
		])

app.run_server(debug=False)  # Turn off reloader if inside Jupyter

