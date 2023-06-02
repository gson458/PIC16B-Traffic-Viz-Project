import requests
import json
import sqlite3
import time
import ast
# import folium
import pandas as pd
from plotly import express as px
from datetime import datetime, timedelta



#create our own database
conn = sqlite3.connect('traffic_data.db')


def extract_coordinates(geo_shape):
    """Helper function to extract latitude and longitude from coordinates"""
    geo_dict = ast.literal_eval(geo_shape)
    coordinates = []

    if geo_dict['type'] == 'Polygon':
        coordinates = geo_dict['coordinates'][0]
    elif geo_dict['type'] == 'MultiPolygon':
        for polygon in geo_dict['coordinates']:
            coordinates.extend(polygon[0])
    elif geo_dict['type'] == 'Point':
        coordinates = [geo_dict['coordinates']]

    latitudes = [coord[1] for coord in coordinates]
    longitudes = [coord[0] for coord in coordinates]
    return latitudes, longitudes

def initialize_db(conn):
    """Creates a counties table in the database if one does not already exist"""
    cmd =\
    """
    SELECT name FROM sqlite_master WHERE name='counties'
    """
    cursor = conn.cursor()
    cursor.execute(cmd)
    if len(cursor.fetchall()) < 1:
        url = "https://public.opendatasoft.com/explore/dataset/us-county-boundaries/download/?format=csv&refine.statefp=06&location=2,40.61349,40.02538&timezone=America/Los_Angeles&lang=en&use_labels_for_header=true&csv_separator=%3B"

        df = pd.read_csv(url, sep=";")
        new_df = df[["NAME", "Geo Shape"]].copy()

        # Create a list to store county bounds dataframes
        county_bounds_list = []

        # Iterate over each county
        for _, row in new_df.iterrows():
            county_name = row['NAME']
            latitudes, longitudes = extract_coordinates(row['Geo Shape'])

            # Calculate the minimum and maximum latitude and longitude for the county
            min_latitude = min(latitudes)
            max_latitude = max(latitudes)
            min_longitude = min(longitudes)
            max_longitude = max(longitudes)

            # Create a dataframe for the county bounds
            county_bounds_df = pd.DataFrame({'County': county_name,
                                            'Min_Latitude': min_latitude,
                                            'Max_Latitude': max_latitude,
                                            'Min_Longitude': min_longitude,
                                            'Max_Longitude': max_longitude}, index=[0])

            # Append the county bounds dataframe to the list
            county_bounds_list.append(county_bounds_df)

        # Concatenate all county bounds dataframes into a single dataframe
        county_bounds = pd.concat(county_bounds_list, ignore_index=True)

        # Sort the DataFrame by county name
        county_bounds = county_bounds.sort_values('County')

        county_bounds.to_sql("counties", conn, if_exists="replace", index=False)

def update(conn):
    """Updates incidents table by removing incidents with end times five hours before current time"""

    try:
        cutoffTime = datetime.now() - timedelta(hours=5)
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

def get_traffic_data(bbox, mapquest_key, google_key):
    """retrieves traffic incident data in a given bounding box from MapQuest Traffic API"""

    # key = ""
    response = requests.get(f"https://www.mapquestapi.com/traffic/v2/incidents?key={mapquest_key}&boundingBox={bbox}&filters=congestion,incidents,construction,event")
    data = pd.DataFrame(response.json()["incidents"])
    if len(data) > 0:
        data = data[['id', 'type', 'severity', 'shortDesc', 'lat', 'lng', 'startTime', 'endTime']]
        addresses = []
        for _, row in data.iterrows():
            coord = str(row['lat']) +","+ str(row['lng'])
            try:
                response = requests.get(f"https://maps.googleapis.com/maps/api/geocode/json?latlng={coord}&key={google_key}")
                addr = response.json()['results'][0]['formatted_address']
            except:
                addr = ""
            addresses.append(addr)
        data['address'] = addresses
    return data

def store_traffic_data(conn, bbox, mapquest_key, google_key):
    """Iterates over a given area and updates traffic incident database by MapQuest API calls"""

    lat_start, lat_end, lng_start, lng_end = bbox
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
        data = get_traffic_data(f"{bbox['lat_start']},{bbox['lng_start']},{bbox['lat_end']},{bbox['lng_end']}", mapquest_key, google_key)
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

def get_counties(conn):
    """Returns a list of all counties in California"""

    cmd =\
    """
    SELECT county FROM counties
    """
    cursor = conn.cursor()
    cursor.execute(cmd)
    return [i[0] for i in cursor.fetchall()]

def get_county_incidents(conn, county):
    """Returns a dataframe of all traffic incidents in given county in California"""

    cmd =\
    f"""
    SELECT Min_Latitude, Max_Latitude, Min_Longitude, Max_Longitude FROM counties
    WHERE County LIKE '{county.upper()}'
    """
    # print(cmd)
    cursor = conn.cursor()
    cursor.execute(cmd)
    # min_lat, max_lat, min_lng, max_lng = cursor.fetchone()
    bbox = cursor.fetchone()

    return get_incidents_in_area(conn, bbox)

def update_county_incidents(conn, key, county):
    """Updates the database with current incidents in a given CA county"""

    cmd =\
    f"""
    SELECT Min_Latitude, Max_Latitude, Min_Longitude, Max_Longitude FROM counties
    WHERE County LIKE '{county.upper()}'
    """
    # print(cmd)
    cursor = conn.cursor()
    cursor.execute(cmd)
    # min_lat, max_lat, min_lng, max_lng = cursor.fetchone()
    bbox = cursor.fetchone()
    store_traffic_data(conn, key, bbox)

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

def incident_scattermap(incidents, **kwargs):
    """Creates a plotly scattermap of traffic incidents"""
    
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

def incident_heatmap(incidents, **kwargs):
    """Creates a plotly heatmap of traffic incidents"""

    fig = px.density_mapbox(incidents,
                            lat="lat",
                            lon="lng",
                            hover_name="id",
                            hover_data=['shortDesc', 'type'],
                            mapbox_style="open-street-map",
                            **kwargs)
    fig.update_layout(margin={"r":0, "l":0,"b":0,"t":0})
    return fig