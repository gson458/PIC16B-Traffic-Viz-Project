# Import Libraries
import requests
import json
import sqlite3
import time
import folium
import pandas as pd
import credentials as cred
from plotly import express as px
from dash import Dash, dcc, html, Input, Output  

# Non-Relevant-Data for now 
# Need to be (queried) database in the future 
conn = sqlite3.connect('traffic_data.db')

def insert_data(conn, data):
    """Inserts new traffic data into incidents table in database"""

    if len(data) > 0:
        data.to_sql("incidents", conn, if_exists="append", index=False)

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


app = Dash(__name__)

# Declare server for Heroku deployment. Needed for Procfile.
server = app.server


app.layout = html.Div([

    html.H1("Real Time Incidents By State", style={'text-align': 'center'}),

    dcc.Input(
        id="state",
        type="text",
        placeholder="State",
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
    Input(component_id='state', component_property='value'),
    ]
)
def update_graph(state):    
    container = f"The input from the user was: {state}"
    state_bounds = pd.read_csv("https://gist.githubusercontent.com/a8dx/2340f9527af64f8ef8439366de981168/raw/81d876daea10eab5c2675811c39bcd18a79a9212/US_State_Bounding_Boxes.csv")
    state_bounds = state_bounds[['NAME', 'STUSPS', 'xmin', 'ymin', 'xmax', 'ymax']]
    state_bounds = state_bounds.rename(columns={"xmin": "min_lng", "xmax": "max_lng", "ymin": "min_lat", "ymax": "max_lat"})
    ma_minlat = state_bounds.loc[state_bounds['STUSPS']== state]['min_lat'].values[0]
    ma_maxlat = state_bounds.loc[state_bounds['STUSPS']==state]['max_lat'].values[0]
    ma_minlng = state_bounds.loc[state_bounds['STUSPS']==state]['min_lng'].values[0]
    ma_maxlng = state_bounds.loc[state_bounds['STUSPS']==state]['max_lng'].values[0]
    conn = sqlite3.connect('traffic_data.db')
    key = "Sks7L0lksbFj0xPNyBdglVFjmsAJGJCU"
    store_traffic_data(conn=conn, lat_start=ma_minlat, lat_end=ma_maxlat, lng_start=ma_minlng, lng_end=ma_maxlng)
    bbox = (ma_minlat, ma_maxlat, ma_minlng, ma_maxlng)
    incidents = get_incidents_in_area(conn, bbox)   
    fig = display_map_with_incidents(incidents, zoom=6)
    return container, fig

if __name__ == '__main__':
    app.run_server(debug=True)