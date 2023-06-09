import requests
import sqlite3
import time
import dash
import pandas as pd
from plotly import express as px
import dash
from dash import dcc, html, Input, Output, callback 
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

dash.register_page(__name__)


def insert_data(conn, data):
    """Inserts new traffic data into incidents table in database"""

    if len(data) > 0:
        data.to_sql("incidents", conn, if_exists="replace", index=False)

def get_traffic_data(bbox):
    """retrieves traffic incident data in a given bounding box from MapQuest Traffic API"""

    key = "Sks7L0lksbFj0xPNyBdglVFjmsAJGJCU"
    response = requests.get(f"https://www.mapquestapi.com/traffic/v2/incidents?key={key}&boundingBox={bbox}&filters=congestion,incidents,construction,event")
    data = pd.DataFrame(response.json()["incidents"])
    if len(data) > 0:
        data = data[['id', 'type', 'severity', 'shortDesc', 'lat', 'lng']]
    return data

def store_traffic_data(conn, lat_start, lat_end, lng_start, lng_end):
    """Iterates over a given area and updates traffic incident database by MapQuest API calls"""

    bbox_step = 1  # Bbox step size for iterating over the U.S. 
    bbox_range = {
        # Starting latitude for bbox
        "lat_start": lat_start, # 24.396308,  #southernmost
        # Ending latitude for bbox
        "lat_end": lat_end, # 49.384358,    #northernmost
        # Starting longitude for bbox
        "lng_start": lng_start, # -125.000000,  #westernmost
        # Ending longitude for bbox
        "lng_end": lng_end # -66.934570    #easternmost
    }
    # create_table()
    bbox = {
        "lat_start": bbox_range["lat_start"],
        "lat_end": bbox_range["lat_start"] + bbox_step,
        "lng_start": bbox_range["lng_start"],
        "lng_end": bbox_range["lng_start"] + bbox_step
    }
    page = 1
    # a loop that continues until the latitude of the current 
    #bounding box exceeds the northernmost latitude of the bbox_range.
    while bbox["lat_start"] <= bbox_range["lat_end"]:
        #to fetch traffic incident data for the current bounding box
        data = get_traffic_data(f"{bbox['lat_start']},{bbox['lng_start']},{bbox['lat_end']},{bbox['lng_end']}")
        insert_data(conn, data)
        print(f"Page {page} processed.")
        page += 1
        #the longitude values of the bounding box are updated by adding the bbox_step 
        #value to both the starting and ending longitudes
        bbox["lng_start"] += bbox_step
        bbox["lng_end"] += bbox_step
        #If the updated longitude exceeds the easternmost longitude of the bbox_range, 
        #the latitude values are updated, and the longitude values are reset to the starting values
        if bbox["lng_start"] > bbox_range["lng_end"]:
            bbox["lat_start"] += bbox_step
            bbox["lat_end"] += bbox_step
            bbox["lng_start"] = bbox_range["lng_start"]
            bbox["lng_end"] = bbox_range["lng_start"] + bbox_step
        time.sleep(1)  # Sleep for 1 second to avoid hitting API rate limits

def get_incidents_in_area(conn, bbox):
    """Retrieves incidents within the given bounding box from database"""
    
    min_lat, max_lat = bbox[0], bbox[1]
    min_lng, max_lng = bbox[2], bbox[3]
    cmd=\
        f"""
            SELECT * FROM incidents
            WHERE lat BETWEEN {min_lat} AND {max_lat}
            AND lng BETWEEN {min_lng} AND {max_lng}
        """
    df = pd.read_sql_query(cmd, conn)
    return df

def display_map_with_incidents(incidents, **kwargs):
    """Creates a plotly map of traffic incidents"""
    fig = px.scatter_mapbox(incidents,
                            lat="lat",
                            lon="lng",
                            color="severity",
                            hover_name="id",
                            hover_data=['shortDesc', 'type'],
                            mapbox_style="open-street-map",
                            **kwargs)
    return fig


def blank_figure():
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

layout =  dbc.Container([
    html.H1("Real Time Incidents by County", className="border rounded-pill my-3 p-2 text-center"),
    
    dbc.Row([
        dbc.Col(
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
    
    html.Div(id='county_output_container', children=[],className="text-danger"),
    
    dbc.Row([
        dbc.Col(dcc.Graph(id="my_county", className="border mt-3 shadow", figure = blank_figure())),
    ],)
])

# Connect the Plotly graphs with Dash Components
@callback(
    [
    Output(component_id='county_output_container', component_property='children'),
    Output(component_id='my_county', component_property='figure')
    ],
    [
    Input(component_id='county', component_property='value'),
    ]
)

def update_graph(county):    
    county_bounds = pd.read_csv("source/LA_counties.csv")
    maxlat= county_bounds[county_bounds['County'] == county]['Max Latitude'].iloc[0]
    minlat= county_bounds[county_bounds['County'] == county]['Min Latitude'].iloc[0]
    minlng= county_bounds[county_bounds['County'] == county]['Min Longitude'].iloc[0]
    maxlng= county_bounds[county_bounds['County'] == county]['Max Longitude'].iloc[0]
    conn = sqlite3.connect('source/county_traffic_data.db')
    store_traffic_data(conn=conn, lat_start=minlat, lat_end=maxlat, lng_start=minlng, lng_end=maxlng)
    bbox = (minlat, maxlat, minlng, maxlng)
    incidents = get_incidents_in_area(conn, bbox)   
    fig = display_map_with_incidents(incidents, zoom=6)
    message = f"There are {len(incidents)} incidents in the area"
    
    return message, fig