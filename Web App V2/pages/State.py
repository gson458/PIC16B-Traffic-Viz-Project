import requests
import sqlite3
import time
import dash
import pandas as pd
from plotly import express as px
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

dash.register_page(__name__) # this is our homepage 



def blank_figure():
    fig = go.Figure(go.Scattermapbox())

    fig.update_layout(
        mapbox = dict(
            style = "stamen-terrain",
            center= go.layout.mapbox.Center(
                lat=40,
                lon=-100
            ),
            zoom = 2.5
        ),
        showlegend = False
        )
    
    return fig


layout =  dbc.Container([
    html.H1("Real Time Incidents by State (NOT FUNCTIONAL)", className="border rounded-pill my-3 p-2 text-center"),
    
    dbc.Row([
        dbc.Col(
            dcc.Input(
                id="state",
                type="text",
                placeholder="State",
                className="mt-auto bg-light border",
            ), 
            width = "auto",
            align = "end"       
            )
        ],
        justify="end",
        ),

    html.Div(id='output_container', children=[],className="text-danger"),
    
    dbc.Row([
        dbc.Col(dcc.Graph(id="my_map", className="border mt-3 shadow", figure = blank_figure())),
    ],)
])

# Connect the Plotly graphs with Dash Components
@callback(
    [
    Output(component_id='output_container', component_property='children'),
    Output(component_id='my_map', component_property='figure')
    ],
    [
    Input(component_id='state', component_property='value'),
    ]
)
def update_graph(state):    
    # state_bounds = pd.read_csv("source/state_bounds.csv")
    # minlat = state_bounds.loc[state_bounds['STUSPS']== state]['min_lat'].values[0]
    # maxlat = state_bounds.loc[state_bounds['STUSPS']==state]['max_lat'].values[0]
    # minlng = state_bounds.loc[state_bounds['STUSPS']==state]['min_lng'].values[0]
    # maxlng = state_bounds.loc[state_bounds['STUSPS']==state]['max_lng'].values[0]
    # conn = sqlite3.connect('source/state_traffic_data.db')
    # store_traffic_data(conn=conn, lat_start=minlat, lat_end=maxlat, lng_start=minlng, lng_end=maxlng)
    # bbox = (minlat, maxlat, minlng, maxlng)
    # incidents = get_incidents_in_area(conn, bbox)   
    # fig = display_map_with_incidents(incidents, zoom=6)
    # message = f"There are {len(incidents)} incidents in the area"
    message = ""
    fig = blank_figure()
    return message, fig