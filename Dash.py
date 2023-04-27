# Import Libraries
import pandas as pd
import plotly.express as px  
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output  


# Non-Relevant-Data for now 
# Need to be (queried) database in the future 
df = pd.read_csv("https://raw.githubusercontent.com/Coding-with-Adam/Dash-by-Plotly/master/Other/Dash_Introduction/intro_bees.csv")
df = df.groupby(['State', 'ANSI', 'Affected by', 'Year', 'state_code'])[['Pct of Colonies Impacted']].mean()
df.reset_index(inplace=True)


# App layout
app = Dash(__name__)
app.layout = html.Div(
    [
        # Website Title 
        html.H1("Plotly Dash Demonstration", style={'text-align': 'center'}),

        # Take User Input 
        dcc.Dropdown(id="slct_year",
                 options=[
                     {"label": "2015", "value": 2015},
                     {"label": "2016", "value": 2016},
                     {"label": "2017", "value": 2017},
                     {"label": "2018", "value": 2018}],
                 multi=False,
                 value=2015,
                 style={'width': "40%"}
                 ),

        html.Div(id='output_container', children=[]),
        html.Br(),

        dcc.Graph(id='my_map', figure={})
    ]
)
    
# Connect the Plotly graphs with Dash Components
@app.callback(
    [
        Output(component_id='output_container', component_property='children'),
        Output(component_id='my_map', component_property='figure')
        ],
    [
        Input(component_id='slct_year', component_property='value')
        ]
)

def update_graph(option_slctd):
    print(option_slctd)
    print(type(option_slctd))

    container = "The input from the user was: {}".format(option_slctd)

    dff = df.copy()
    dff = dff[dff["Year"] == option_slctd]
    dff = dff[dff["Affected by"] == "Varroa_mites"]
    
    # Plotly Graph Objects (GO)
    fig = go.Figure(
        data=[go.Choropleth(
            locationmode='USA-states',
            locations=dff['state_code'],
            z=dff["Pct of Colonies Impacted"].astype(float),
            colorscale='Reds',
        )]
    )
    
    fig.update_layout(
        title_text="Fancy Title for the Map",
        title_xanchor="center",
        title_font=dict(size=24),
        title_x=0.5,
        geo=dict(scope='usa'),
    )

    return container, fig


# Run Dash App 
if __name__ == '__main__':
    app.run_server(debug=True)