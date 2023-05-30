import requests
import json
import sqlite3
import time
import folium
import pandas as pd
from plotly import express as px
from datetime import datetime, timedelta



#create our own database
conn = sqlite3.connect('traffic_data.db')

def update(conn):
    """Updates incidents table by removing incidents with end times five hours before current time"""

    try:
        cutoffTime = datetime.now() + timedelta(hours=24)
        cursor = conn.cursor()
        cmd=\
        f"""
        DELETE FROM incidents
        WHERE endDatetime < '{cutoffTime.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        cursor.execute(cmd)
    except:
        print("Error, likely table does not exist")


def insert_data(conn, data):
    """Inserts new traffic data into incidents table in database"""
    
    if len(data) <= 0:
        return
    cursor = conn.cursor()
    data['endDatetime'] = data['endTime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
    # checkcmd = "SHOW TABLES LIKE 'testtable'"
    # cursor.execute(checkcmd)
    # result = cursor.fetchone()
    try:
        data.to_sql("intermediate", conn, if_exists='replace', index=False)
        
        # incidents table is replaced here with a test table
        cmd=\
        """
        DELETE FROM intermediate
        WHERE id IN (SELECT id FROM incidents)
        """
        cursor.execute(cmd)
        cursor = conn.cursor()
        cmd=\
        """
        INSERT INTO incidents
        SELECT * FROM intermediate
        """
        cursor.execute(cmd)
        cursor = conn.cursor()
        cmd = "DROP TABLE intermediate"
        cursor.execute(cmd)
    except:
        print('except triggered')
        data.to_sql("incidents", conn, if_exists="append", index=False)

def get_traffic_data(bbox, key):
    """retrieves traffic incident data in a given bounding box from MapQuest Traffic API"""

    # key = ""
    response = requests.get(f"https://www.mapquestapi.com/traffic/v2/incidents?key={key}&boundingBox={bbox}&filters=congestion,incidents,construction,event")
    data = pd.DataFrame(response.json()["incidents"])
    if len(data) > 0:
        data = data[['id', 'type', 'severity', 'shortDesc', 'lat', 'lng', 'startTime', 'endTime']]
    return data

def store_traffic_data(conn, key, lat_start, lat_end, lng_start, lng_end):
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
        data = get_traffic_data(f"{bbox['lat_start']},{bbox['lng_start']},{bbox['lat_end']},{bbox['lng_end']}", key)
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
    print(cmd)
    df = pd.read_sql_query(cmd, conn)
    return df

def display_map_with_incidents(incidents, **kwargs):
    """Creates a plotly map of traffic incidents"""
    
    # try:
    #     center_lat, center_lng = incidents['lat'][0], incidents['lng'][0]
    # except:
    #     print("data frame error?")
    #     center_lat, center_lng = 0, 0

    fig = px.scatter_mapbox(incidents,
                            lat="lat",
                            lon="lng",
                            color="severity",
                            hover_name="id",
                            hover_data=['shortDesc', 'type'],
                            mapbox_style="open-street-map",
                            **kwargs)
    fig.update_layout(margin={"r":0, "l":0,"b":0,"t":0})
    return fig