import pandas as pd
import os, sys

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go # graph objects
import plotly.express as px

# imports for dash construction
import pydecovid
from pydecovid.dashutil.tableutil import *
from pydecovid.queries import qry_table1


# --------- "GLOBALS" -------------------------------------------------
main_text_style = {'text-align': 'center', 'max-width': '800px', 'margin':'auto'}
main_div_style = {'margin':'auto', 'padding-left': '100px', 'padding-right':'100px',
                  'padding-top':'20px'}


# --------- DATA ------------------------------------------------------

# Load Achilles Results
# ach_res = pd.read_feather(file_achilles_res)
ach_res = pd.read_csv('https://srv-file18.gofile.io/download/ONDJ6G/achilles_results.csv')

# Perform specific transformations for Table 1
tbl_one = qry_table1.query(ach_res)
tbl_one.columns = ['', 'N', '%']
assert tbl_one.shape[0] <= 24, "REQ UPDATE: Table length hard-coded in app.layout for legacy reasons."
title_rows = (tbl_one.apply(lambda x: sum([len(d) > 0 for d in x]), axis=1) == 1).values


# --------- FIGURES ---------------------------------------------------

df_age = qry_table1.achilles_age_agg(ach_res)
df_gender = qry_table1.achilles_gender(ach_res)

fig_age = go.Figure()
fig_gender = go.Figure()


fig_age.add_trace(
    go.Bar(
        x = df_age.stratum_1, y = df_age.count_value, marker_color=px.colors.qualitative.T10[0])   #, name='SF'),
)


cols_T10_permute = [px.colors.qualitative.T10[i] for i in [3,1,0,2,4,5,6,7,8,9]]
fig_gender = px.pie(df_gender, values='count_value', names='stratum_1', color='stratum_1',
             color_discrete_sequence=cols_T10_permute, hole=0.3)
# fig_gender.add_trace(
#     go.Bar(
#         x = df_gender.stratum_1, y = df_gender.count_value)
# )


titles = ['Age', 'Gender']
for fig, tt in zip([fig_age, fig_gender], titles):
    fig.update_layout(
        template='none',
        title='{:s} Distribution of Person Table'.format(tt),
        title_x=0.5,   # horizontal centering
        autosize=True,
        margin=dict(l=50,r=50,b=50,t=50,pad=4),
        font=dict(size=10)
    )
    # fig.update_layout(
    #     height=200
    # )


# --------- COPY ------------------------------------------------------

introduction = '''
### Data landing page

Welcome to the dummy landing page! See the dashboard [GUIDE-(TO-DO)](deadlink) if it's your first time here.

Data are from {:d} patients, pulled from the [SYNPUF](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF)
[(1k subset)](http://www.ltscomputingllc.com/downloads/). All numbers (including the previous one) are
dynamic and pulled from an OMOP standard database schema. The dashboard is currently set up to process
the results of the [Achilles](https://github.com/OHDSI/Achilles) tool which creates an OLAP-style results
table. This results table is joined to the Concept table containing the default vocabularies suggested by
[Athena](https://athena.ohdsi.org/vocabulary/list) which dynamically creates the 'Table 1' on the left.

'''.format(ach_res.iloc[0,6])



# --------- CONSTRUCT APP ---------------------------------------------
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(children=[
    dcc.Markdown(children=introduction, style=main_text_style, className="row"),
    html.Div([

        # column 1
        html.Div(generate_table(tbl_one.iloc[:24, :], title_rows[:24]),
            className="four columns", style={'height': '500px', 'vertical-align':'middle',
              'display': 'flex', 'justify-content': 'center', 'align-items': 'center'}),
        # html.Div(generate_table(tbl_one.iloc[24:, :], title_rows[24:]), className="four columns")

        # column 2
        html.Div([
            dcc.Graph(
                id='example-graph',
                figure=fig_age,
                className="row", style={'height': '250px', 'margin': '0px'}
            ),
            dcc.Graph(
                id='example-graph2',
                figure=fig_gender,
                className="row", style={'height': '250px', 'margin': '0px'}
            )
        ], className="eight columns")
        ], className="row", style=main_div_style)
])

if __name__ == '__main__':
    app.run_server(debug=True)
