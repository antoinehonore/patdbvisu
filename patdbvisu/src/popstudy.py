from startup import app,engine
from src.utils import gentbl_raw

import pandas as pd
import dash
from dash import dcc, Input, Output,html
from dash.exceptions import PreventUpdate


def get_categories():
    with engine.connect() as con:
        l = pd.read_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'view__uid_has'",
            con)["column_name"].values.tolist()
    return [{"label": k, "value": k} for k in [ll for ll in l if ll != "ids__uid"]]


def new_checklist(i, init_val=None):
    return dcc.Dropdown(
        options=get_categories(),
        value=init_val,
        # labelStyle={"display": "inline-block"},
        id="checklist-{}".format(i),
        multi=True,
        placeholder="Population {}: Write the categories you want".format(i),
        style=dict(width="450px")
    )


def cond_from_checklist(value):
    all_categories = get_categories()
    return " and ".join([v + "=1" for v in value]
                        + [v["value"] + "=0" for v in all_categories if not (v in all_categories)]
                        )

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
        if len(checklist) > 0:
            init_val = checklist[-1]["props"]["value"]
            pass
        return checklist + [new_checklist(len(checklist) + 1, init_val=init_val)]


@app.callback(
    Output(component_id="div-checklists-results", component_property="children"),
    Output(component_id="downloadchecklists", component_property="data"),
    Input(component_id="updatechecklists-button", component_property='n_clicks'),
    Input(component_property="children", component_id="div-checklists"),
    Input(component_id="downloadchecklists-button", component_property='n_clicks'),
)
def update_checklist_test(n_clicks, checklists, dl_click):
    if (n_clicks is None) and (dl_click is None):
        raise PreventUpdate
    else:
        OUT = []
        DF = []
        isempty = []
        for i, v in enumerate(checklists):
            if len(v["props"]["value"]) > 0:
                thequery = "select ids__uid from view__uid_has where {}".format(
                    cond_from_checklist(v["props"]["value"]))

                with engine.connect() as con:
                    dout = pd.read_sql(thequery, con).values.reshape(-1)
                    doverview = pd.read_sql("select * from overview where ids__uid in ({});".format(
                        "\'" + "\',\'".join(dout.tolist()) + "\'"), con)
                    doverview["group"] = "_".join(v["props"]["value"])
                # doverview.to_csv("test.csv", sep=";", index=False)
                DF.append(doverview)
                OUT.append("\n".join(dout.tolist()))
            else:
                isempty.append(i + 1)
        ctx = dash.callback_context

        if not ctx.triggered:
            button_id = 'No clicks yet'
        else:
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        # print(button_id)

        # print(len(DF), isempty, len(isempty), len(checklists))

        if len(isempty) > 0:
            return [html.P("\!/ Empty category: {}".format(", ".join(list(map(str, isempty)))))], None
        else:
            dout = pd.concat(DF, axis=0)

            if button_id == "downloadchecklists-button":
                return "Download", dcc.send_data_frame(dout.to_excel, filename="PopulationsOverview.xlsx")
            else:
                return [gentbl_raw(dout, id="popstudy-output", page_size=10, style_table={'overflowX': 'auto'})
                        ], None
