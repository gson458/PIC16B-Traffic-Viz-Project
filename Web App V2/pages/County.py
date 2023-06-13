import requests
import sqlite3
import time
import dash
import pandas as pd
from datetime import datetime
from plotly import express as px
import dash
from dash import dcc, html, Input, Output, callback, State, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from pages.traffic_data import initialize_db
from pages.traffic_data import update_county_incidents
from pages.traffic_data import get_county_incidents
from pages.traffic_data import incident_scattermap
from pages.traffic_data import incident_heatmap


# This file manages the county page of the web app.


# Sets this as the homepage
dash.register_page(__name__, path='/')


def blank_figure():
    """Creates a blank plotly scattermapbox"""
    fig = go.Figure(go.Scattermapbox())

    fig.update_layout(
        mapbox = dict(
            style = "stamen-terrain",
            center= go.layout.mapbox.Center(
                lat=38,
                lon=-120
            ),
            zoom = 3.6
        ),
        showlegend = False
        )
    
    return fig

# Page Layout
layout =  dbc.Container([
    html.H1("Real Time Incidents by County", className="border rounded-pill my-3 p-2 text-center"),
    
    dbc.Row([
        dbc.Col(
            # County text box input located in the upper right of the page
            dcc.Input(
                id="county",
                type="text",
                placeholder="County",
                className="mt-auto bg-light border",
            ), width = "auto",
        )
        ],
            justify="end"
            ),
    
    # Red message in the top left of the page that describes how many incidents are located in the inputted county
    html.Div(id='county_output_container', children=[],className="text-danger"),
    # Red message in the top left of the page that displays the last time the database was updated (if in the same session)
    html.Div(id='refresh_message', children=[],className="text-danger"),
    # Button in the top left that triggers the second callback updating the database in the selected county
    html.Button('Update county incidents', id='refresh_button', n_clicks=0),
    
    dbc.Row([
        # The plotly figure that displays all relevant incidents on a map
        dbc.Col(dcc.Graph(id="my_county", className="border mt-3 shadow", figure = blank_figure())),
    ]),
    
    dbc.Row([
        # Dropdown menu below the map that allows you to select between scatter_mapbox and scatter_heatmap modes
        dbc.Label('Map Type:'),
        dbc.Col(dcc.Dropdown(
            id="map_type",
            options=["scatter_mapbox", "scatter_heatmap"],
            value="scatter_mapbox",
            clearable=False,
        )),
    ],),

    dbc.Row([
        # Table that displays all incidents in the county inputted
        dbc.Label('Incidents:'),
        dbc.Col(dash_table.DataTable(id='table', data=[], page_size=20)),
    ],)
])

# Connect the Plotly graphs with Dash Components
@callback(
    [
    Output(component_id='county_output_container', component_property='children'),
    Output(component_id='my_county', component_property='figure'),
    Output(component_id='table', component_property='data')
    ],
    [
    Input(component_id='county', component_property='value'),
    Input(component_id='map_type', component_property='value'),
    Input(component_id="refresh_message", component_property='children')
    ]
)

def update_graph(county, map_type, update_callback_trigger):
    """Updates the page based on changes to the county text input"""
    
    conn = sqlite3.connect('webapptdb.db')   
    try:
        # Retrieves incidents of input county
        initialize_db(conn)
        incidents = get_county_incidents(conn, county)
        # Selects correct map type
        if map_type=='scatter_mapbox':
            fig = incident_scattermap(incidents, zoom=8, color_continuous_scale='bluered')
        else:
            fig = incident_heatmap(incidents, zoom=8, radius=8)
        message = f"There are {len(incidents)} incidents in the area"
    except:
        message = f"Cannot update figure; updating or invalid input"
        fig = blank_figure()

    conn.close()

    # Converts incidents dataframe into a dictionary for display in the web app datatable
    try:
        incidents = incidents.drop(columns=['id', 'lat', 'lng', 'startTime', 'endTime', 'county'])
        tableInfo = incidents.to_dict('records')
    except:
        tableInfo = None
    
    return message, fig, tableInfo

# Triggers upon pressing Update county incidents button
@callback(
    [
        Output(component_id="refresh_message", component_property='children')
    ],
    [
        Input(component_id='refresh_button', component_property='n_clicks')
    ],
    [
        State(component_id='county', component_property='value')
    ],
    prevent_initial_call=True
)

def update_incidents(n_clicks, county):
    """Updates the database for new incidents in county text input"""

    conn = sqlite3.connect('webapptdb.db')
    initialize_db(conn)
    update_county_incidents(conn, county)
    conn.close()

    # Gets the current time for display in the relevant message
    time = str(datetime.now())
    return [f"Database last updated on {time} at {county.upper()} county"]
    