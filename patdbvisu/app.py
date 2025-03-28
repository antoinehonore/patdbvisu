from src.utils import thestyle, nav_bar_style, get_latest_update, create_completion_dropdown
import socket
from dash import dcc, html
from startup import app, server
from src.dbstatus import update_output, update_completion_data, showhide_db_details
from src.popstudy import update_check_lists, update_checklist_test
from src.patdisp import cb_render, plot_patient, display_patient_interv


separator = html.Img(src=app.get_asset_url('line.png'), style={"width": "100%", "height": "5px"})

navbar = html.Div(children=[
    html.Div([
        html.H1("- Oh My DB ! -", style={'text-align': 'center'}),
        dcc.Markdown('[Documentation](https://gitlab.com/antoinehonore/patdbvisu/-/wikis/documentation#user-information) - [Submit an issue](https://gitlab.com/antoinehonore/patdbvisu/-/issues)', style={'text-align': 'center'}),
    ]),
    separator,
    html.H2("Database status", style={'text-align': 'left'}),
    html.Div([
        html.Button('Update', id='refresh-button', style=thestyle),
        html.Div(get_latest_update("update-status", style=thestyle)),
        html.Div([
            html.H3("Database size: "),
            html.P("-", id="dbinfo-size"),
            html.P("-", id="dbinfo-npat")
        ], style=thestyle),
        html.Button('More', id='moreless-button', style=thestyle)
    ], style={'display': 'flex', 'flex-direction': 'row'}),
    html.Div([
        html.H4("Data overlap", style={'text-align': 'left'}),
        html.Div(id="div-db-completion", children=[create_completion_dropdown(),
                                                   html.P("-", id="completion-result")]),
        html.Div(id="div-db-details"),
    ])
], style=nav_bar_style)
main_layout = html.Div([
    separator,
    navbar,
    separator,
    html.H2("Population study", style={'text-align': 'left'}),
    dcc.Checklist(options=[{"label": 'Data collection', "value": "is_datacollection"}], value=[], id="popstudy-datacollection"),
    html.Div([
        html.Button('More', id='popstudy-morechecklist-button'),
        html.Button('Less', id='popstudy-lesschecklist-button'),
        html.Button('Stats', id='popstudy-updatechecklists-button'),
        html.Div([html.Button("Download raw", id="popstudy-downloadchecklists-button"),
                  dcc.Download(id="popstudy-downloadchecklists")]
                 ),
        html.Div(id="popstudy-outputtxt-div")
    ], style={'display': 'flex', 'flex-direction': 'row'}),
    dcc.Markdown("[Definition of the categories](https://gitlab.com/antoinehonore/patdbvisu/-/wikis/documentation#definition-of-categories)"),
    html.Div([
        html.Div(id="popstudy-checklists-div", children=[]),
        html.Div(id="popstudy-checklists-results-div", children=[])
    ]),
    separator,
    html.H2("Patient Display", style={'text-align': 'left'}),
    html.Div([
        dcc.Input(id="patdisp-input-patid", placeholder="Write Patient ID", style=dict(width="450px")),
        html.Button('Search', id='patdisp-search-button'),
        html.Button('Convert', id='patdisp-convert-button'),
        html.Button('Clear', id='patdisp-clear-button'),
        html.Div([html.Button('Plot', id='patdisp-plot-button'),
                  dcc.Checklist(options=[{"label": 'All available LF', "value": "available_lf"},
                                         {"label": 'All available HF', "value": "waveform"},
                                         {"label": 'Respirator', "value": "respirator"}], value=[],
                                id="patdisp-plot-checklist")]
                 , className="row"),
        html.P(id="patdisp-convert-disp"),
        html.Div(id="patdisp-plot-disp")
    ], className="row"),
    html.Div(id="patdisp-div", children=[get_latest_update(id="patdisp-latestupdate"),
                                            html.Div([dcc.Dropdown(options=[],
                                                                   value=[],
                                                                   id="patdisp-interv-dropdown",
                                                                   multi=True,
                                                                   placeholder="Choose the intervals to plot",
                                                                   style=dict(width="450px")
                                                                   ),
                                                      html.Button("Display", id="patdisp-display-button")
                                                      ],
                                                     style={'display': 'flex', 'flex-direction': 'row'}
                                                     ),
                                            html.Div(id="patdisp-figures")
                                    ]
             ),
    separator,
    html.Div([html.Br()] * 10)
])

app.layout = main_layout

if __name__ == "__main__":
    if socket.gethostname() == "cmm0576":
        app.run_server(debug=True)
    else:
        app.run_server(debug=False)
