from startup import app, engine
from src.utils import gentbl_raw

import pandas as pd
import dash
from dash import dcc, Input, Output, html
from dash.exceptions import PreventUpdate

n_field_per_pop = 3


def get_categories():
    with engine.connect() as con:
        l = pd.read_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'view__uid_has'",
            con)["column_name"].values.tolist()
    return [{"label": k,
             "value": k}
            for k in [ll for ll in l if ll != "ids__uid"]
            ]


def new_checklist(i, init_val=None, placeholder=" "):
    return dcc.Dropdown(
        options=get_categories(),
        value=init_val,
        # labelStyle={"display": "inline-block"},
        id="checklist-{}".format(i),
        multi=True,
        placeholder="Population {}: Write the categories you{}want".format(i,placeholder),
        style=dict(width="450px")
    )


def cond_from_checklist(value, i):
    return " and ".join([v + "=1" for v in value] if (i % 2) == 0 else [v + "=0" for v in value])


@app.callback(
    Output(component_property="children", component_id="popstudy-checklists-div"),
    Input(component_id='popstudy-lesschecklist-button', component_property='n_clicks'),
    Input(component_id='popstudy-morechecklist-button', component_property='n_clicks'),
    Input(component_id="popstudy-checklists-div", component_property="children"),
)
def update_check_lists(clickless, clickmore, checklist):
    ctx = dash.callback_context

    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    init_val = []
    init_val_neg = []
    if button_id == "popstudy-lesschecklist-button":
        return checklist[:-3]
    else:
        if len(checklist) > 0:
            init_val_neg = checklist[-2]["props"]["value"]
            init_val = checklist[-1]["props"]["value"]
            pass
        return checklist + [html.Plaintext("Population {}".format(len(checklist)//n_field_per_pop+1)),
                            new_checklist(len(checklist)//n_field_per_pop + 1, init_val=init_val),
                            new_checklist("neg-{}".format(len(checklist)//n_field_per_pop + 1), init_val=init_val_neg, placeholder=" DON'T ")
                            ]

from tableone import TableOne
import pandas as pd
from dash import dash_table as dt

"""
Pollard TJ, Johnson AEW, Raffa JD, Mark RG (2018). tableone: An open source
    Python package for producing summary statistics for research papers.
    JAMIA Open, Volume 1, Issue 1, 1 July 2018, Pages 26-31.
    https://doi.org/10.1093/jamiaopen/ooy012
"""
@app.callback(
    Output(component_id="popstudy-checklists-results-div", component_property="children"),
    Output(component_id="popstudy-downloadchecklists", component_property="data"),
    Input(component_id="popstudy-updatechecklists-button", component_property='n_clicks'),
    Input(component_id="popstudy-checklists-div", component_property="children"),
    Input(component_id="popstudy-downloadchecklists-button", component_property='n_clicks'),
)
def update_checklist_test(n_clicks, checklists, dl_click):
    ctx = dash.callback_context

    if not ctx.triggered:
        raise PreventUpdate


    if ((n_clicks is None) and (dl_click is None)):
        raise PreventUpdate

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if (button_id != "popstudy-updatechecklists-button") and (button_id != "popstudy-downloadchecklists-button"):
        raise PreventUpdate
    print("button pressed ",button_id)

    DF = []
    assert(len(checklists) % n_field_per_pop == 0)
    print(len(checklists)//n_field_per_pop)
    for i in range(len(checklists)//n_field_per_pop):
        v = checklists[n_field_per_pop*i + 1]
        v_not = checklists[n_field_per_pop * i + 2]

        pos_cond = "True"
        if len(v["props"]["value"]) > 0:
            pos_cond= " and ".join([vv + "=1" for vv in v["props"]["value"]])

        neg_cond = "True"
        if len(v_not["props"]["value"]) > 0:
            neg_cond = " and ".join([vv + "=0" for vv in v_not["props"]["value"]])

        thequery = "select ids__uid from view__uid_has where {}".format(pos_cond + " and " + neg_cond)

        #print(thequery)
        with engine.connect() as con:
            dout = pd.read_sql(thequery, con).values.reshape(-1)
            doverview = pd.read_sql("select * from overview where ids__uid in ({});".format(
                "\'" + "\',\'".join(dout.tolist()) + "\'"), con)
        pos = "_".join(v["props"]["value"])
        neg = "not_" + "_".join(v_not["props"]["value"])

        f=[pos,neg]
        f=[ff for ff in f if (ff!="") and (ff != "not_")]

        doverview["group"] = "Population-{} ".format(i+1) + "_and_".join(f)
        if doverview.empty:
            return [html.P("Population {} is empty...".format(i+1)), None]

        DF.append(doverview)

    if all([dd.empty for dd in DF]):
        raise PreventUpdate

    dout = pd.concat(DF, axis=0)
    columns = ["sex", "bw", "ga_w", "apgar_1", "apgar_5", "apgar_10", "group"]
    groupby = ['group']
    nonnormal = None #['bw']
    categorical = ["sex"]
    labels = {'death': 'mortality'}
    dout[groupby] = dout[groupby].applymap(lambda x: x.replace("_"," ") if isinstance(x,str) else x)

    # print(dout)

    mytable = TableOne(dout.reset_index()[columns],
                       columns=columns,
                       categorical=categorical,
                       groupby=groupby,
                       nonnormal=nonnormal,
                       rename=labels,
                       pval=True
                       )

    ddisp = pd.read_csv(StringIO(mytable.to_csv()))
    #print(ddisp)

    #.columns = list(map(str,list(range(ddisp.shape[1]))))
    ddisp.fillna("", inplace=True)
    #ddisp.columns = [s if not ("Unnamed" in s) else "" for s in ddisp.columns]
    #ddisp=ddisp.applymap(lambda s:s.replace("_", " "))

    if button_id == "popstudy-downloadchecklists-button":
        return "Download", dcc.send_data_frame(dout.to_excel, filename="PopulationsOverview.xlsx")
    else:
        return [gentbl_raw(ddisp, id="popstudy-output",
                           style_cell={'border': '1px solid grey', 'textAlign': 'center','minWidth':"20px",'maxWidth':"100px"},
                           style_header={'display': 'none'},
                           style_data={
                               'whiteSpace': 'normal',
                               'width': 'auto',
                               'height':'auto'
                           }, fill_width=False
                           )
                ], None

from io import StringIO