
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