# Import Libraries
import pandas as pd
import sqlite3
import plotly.express as px  
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output  
from sklearn.linear_model import LinearRegression

# Non-Relevant-Data for now 
# Need to be (queried) database in the future 
def query_climate_database(country, year_begin, year_end, month): 
    conn = sqlite3.connect('my_database.db')
    cmd = \
    f"""
    SELECT S.name, S.latitude, S.longitude, C.name, T.year, T.month, T.temp
    FROM temperatures T JOIN stations S ON T.id = S.id JOIN countries C ON T."FIPS 10-4" = C."FIPS 10-4"
    WHERE T.year >= {year_begin} AND T.year <= {year_end} AND T.month = {month} AND C.name = "{country}"
     """
    df = pd.read_sql_query(cmd,conn) 
    return df


app = Dash(__name__)

# Declare server for Heroku deployment. Needed for Procfile.
server = app.server


app.layout = html.Div([

    html.H1("HW 1 to Dash Website", style={'text-align': 'center'}),

    dcc.Input(
        id="country",
        type="text",
        placeholder="Country",
    ),
    dcc.Input(
        id="begin",
        type="number",
        placeholder="Year Begin",
    ),
    dcc.Input(
        id="end",
        type="number",
        placeholder="Year End",
    ),

    html.Div(id='output_container', children=[]),
    html.Br(),

    dcc.Graph(id='my_map', figure={})
])


# Connect the Plotly graphs with Dash Components
@app.callback(
    [
    Output(component_id='output_container', component_property='children'),
    Output(component_id='my_map', component_property='figure')
    ],
    [
    Input(component_id='country', component_property='value'),
    Input(component_id='begin', component_property='value'),
    Input(component_id='end', component_property='value'),
    ]
)
def update_graph(country, begin, end):
    print(country)
    print(type(country))
    container = f"The input from the user was: {country}, {begin}, {end}"

        
    # Create dataframe using query_climate_database() above
    df = query_climate_database(country, begin, end, 1)
    
        
    # Create figure object
    fig = px.scatter_mapbox(df, lat="LATITUDE",
                            lon="LONGITUDE", zoom = 2, mapbox_style = "carto-positron")

    
    return container, fig


if __name__ == '__main__':
    app.run_server(debug=True)