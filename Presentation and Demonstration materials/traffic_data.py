import requests
import json
import sqlite3
import time
import ast
# import folium
import pandas as pd
import asyncio
import aiohttp
from plotly import express as px
from datetime import datetime, timedelta
import credentials as cred
import nest_asyncio

import urllib
from pathlib import Path
from zipfile import ZipFile
import geopandas as gpd

# Allows async api calls when called from a jupyter notebook
# See problems with existing async loops if error returns
nest_asyncio.apply()


#create our own database
conn = sqlite3.connect('traffic_data.db')


# Gets county boundary line information for use in figure
src = [
    {
        "name": "counties",
        "suffix": ".shp",
        "url": "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_5m.zip",
    },
]
data = {}
for s in src:
    f = Path.cwd().joinpath(urllib.parse.urlparse(s["url"]).path.split("/")[-1])
    if not f.exists():
        r = requests.get(s["url"],stream=True,)
        with open(f, "wb") as fd:
            for chunk in r.iter_content(chunk_size=128): fd.write(chunk)

    fz = ZipFile(f)
    fz.extractall(f.parent.joinpath(f.stem))

    data[s["name"]] = gpd.read_file(
        f.parent.joinpath(f.stem).joinpath([f.filename
                                            for f in fz.infolist()
                                            if Path(f.filename).suffix == s["suffix"]][0])
    ).assign(source_name=s["name"])
gdf = pd.concat(data.values()).to_crs("EPSG:4326")
cagdf = gdf[gdf['STATEFP']=='06']





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
    # Works, but do not use; removing old incidents is unnecessary

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
    
    # Converts endTime info into a datetime object, added to new column of dataframe
    if len(data) <= 0:
        print("No data to insert")
        return
    cursor = conn.cursor()
    data['endDatetime'] = data['endTime'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))

    try:
        # Data is temporarily stored in an intermediate table
        data.to_sql("intermediate", conn, if_exists='replace', index=False)
        
        # Delete duplicate data from intermediate table
        cmd=\
        """
        DELETE FROM intermediate
        WHERE id IN (SELECT id FROM incidents)
        """
        cursor.execute(cmd)
        cursor = conn.cursor()

        # Insert remaining data from intermediate table to incidents table
        cmd=\
        """
        INSERT INTO incidents
        SELECT * FROM intermediate
        """
        cursor.execute(cmd)

        # Intermediate table deleted
        cursor = conn.cursor()
        cmd = "DROP TABLE intermediate"
        cursor.execute(cmd)
    except:
        # Triggers usually when incident table does not already exist
        print('Except triggered')
        data.to_sql("incidents", conn, if_exists="append", index=False)



async def get_traffic_data_async(bbox, session):
    """retrieves traffic incident data in a given bounding box from MapQuest Traffic API"""

    try:
        mapquest_key = cred.mapquest_api_key
        url = f"https://www.mapquestapi.com/traffic/v2/incidents?key={mapquest_key}&boundingBox={bbox}&filters=congestion,incidents,construction,event"
        async with session.get(url=url) as response:
            response = await response.json()
            data = pd.DataFrame(response["incidents"])
            print(f"{len(data)} incidents processed in batch")
            return data
    except Exception as e:
        print("Unable to get response (Mapquest), error due to {}".format(e.__class__))
        print(f"Mapquest API Reponse: {response}")


async def store_traffic_data_async(conn, bbox):
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
    page = 0

    async with aiohttp.ClientSession() as session:
        tasks=[]
        while bbox["lat_start"] <= bbox_range["lat_end"]:
            bbox_string = f"{bbox['lat_start']},{bbox['lng_start']},{bbox['lat_end']},{bbox['lng_end']}"
            tasks.append(asyncio.ensure_future(get_traffic_data_async(bbox_string, session)))
            # print(f"Section {page} initiated.")
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
        print(f"{page} Mapquest API calls made.")
        dfs = await asyncio.gather(*tasks)
        data = pd.concat(dfs)
        data = data.drop_duplicates(subset=['id'])
        if len(data) > 0:
            data = data[['id', 'type', 'severity', 'shortDesc', 'lat', 'lng', 'startTime', 'endTime']]

            # Calls Google API for reverse geocoding, contains (nearest) address info for given coordinates
            geocodes = []
            asyncio.run(get_batch_addresses(data, geocodes))

            # Extracts and attaches county and address info from geocode data
            data['geocode'] = geocodes
            data['county'] = data['geocode'].apply(get_county)
            data['address'] = data['geocode'].apply(get_formatted_address)
            data=data.drop(columns=['geocode'])
            insert_data(conn, data)


async def get_address(url, session):
    """Async call that gets the reverse geocode info of a coordinate pair"""

    try:
        async with session.get(url=url) as response:
            resp = await response.json()
            # print(resp)
            return resp['results'][0]
    except Exception as e:
        print(resp)
        print("Unable to get response, error due to {}".format(e.__class__))

async def get_batch_addresses(df, addresses):
    """Async API calls that get reverse geocode info from a set of coordinates"""

    async with aiohttp.ClientSession() as session:
        tasks=[]
        for _, row in df.iterrows():
            coord = str(row['lat']) +","+ str(row['lng'])
            url = f'https://maps.googleapis.com/maps/api/geocode/json?latlng={coord}&key={cred.google_api_key}'
            tasks.append(asyncio.ensure_future(get_address(url, session)))
        addys = await asyncio.gather(*tasks)
        for addr in addys:
            addresses.append(addr)

def get_county(geocode):
    """Extracts the county from geocoded info, if it exists"""

    try:
        info = geocode['address_components']
        # Administrative area level 2 is, by Google's definition, the county
        county_dict = next((item for item in info if 'administrative_area_level_2' in item['types']), None)
        return county_dict['long_name']
    except:
        print("County N/A")
        return ''

def get_formatted_address(geocode):
    """Extracts the formatted address from geocoded info, if it exists"""

    try:
        return geocode['formatted_address']
    except:
        print("Formatted address N/A")
        return ''

def get_traffic_data(bbox):
    """(OBSOLETE: see async version) retrieves traffic incident data in a given bounding box from MapQuest Traffic API"""

    mapquest_key = cred.mapquest_api_key
    response = requests.get(f"https://www.mapquestapi.com/traffic/v2/incidents?key={mapquest_key}&boundingBox={bbox}&filters=congestion,incidents,construction,event")
    data = pd.DataFrame(response.json()["incidents"])
    if len(data) > 0:
        data = data[['id', 'type', 'severity', 'shortDesc', 'lat', 'lng', 'startTime', 'endTime']]

        geocodes = []
        asyncio.run(get_batch_addresses(data, geocodes))
        print(len(geocodes), len(data))

        data['geocode'] = geocodes
        data['county'] = data['geocode'].apply(get_county)
        data['address'] = data['geocode'].apply(get_formatted_address)
        data=data.drop(columns=['geocode'])
    return data

def store_traffic_data(conn, bbox):
    """(OBSOLETE: see async version) Iterates over a given area and updates traffic incident database by MapQuest API calls"""
    # Only use this function to compare speed with async version

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
    SELECT * FROM incidents
    WHERE county LIKE '{county.upper() + " County"}'
    """
    return pd.read_sql_query(cmd, conn)

def update_county_incidents_synchronous(conn, county):
    """(OBSOLETE: see async version) Updates the database with current incidents in a given CA county using its approximate bounding box"""
    # Only use this function for speed demonstration purposes

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
    store_traffic_data(conn, bbox)

def update_county_incidents(conn, county):
    """Updates the database with current incidents in a given CA county using its approximate bounding box"""

    # Retrieves county bounding box
    cmd =\
    f"""
    SELECT Min_Latitude, Max_Latitude, Min_Longitude, Max_Longitude FROM counties
    WHERE County LIKE '{county.upper()}'
    """
    cursor = conn.cursor()
    cursor.execute(cmd)
    bbox = cursor.fetchone()

    # Runs update function
    asyncio.run(store_traffic_data_async(conn, bbox))   

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

    fig = px.scatter_mapbox(incidents,
                            lat="lat",
                            lon="lng",
                            color="severity",
                            hover_name="shortDesc",
                            hover_data=['id', 'type', 'endDatetime'],
                            mapbox_style="open-street-map",
                            **kwargs)
    
    # Adjusts margin and adds layer for county boundaries
    fig.update_layout(
        margin={"r":0, "l":0,"b":0,"t":0},
        mapbox_layers=[
            {
                "source": json.loads(cagdf.geometry.to_json()),
                "below": "traces",
                "type": "line",
                "color": "black",
                "line": {"width": 1},
            },
        ],
        mapbox_style="open-street-map",
    )
    return fig

def incident_heatmap(incidents, **kwargs):
    """Creates a plotly heatmap of traffic incidents"""

    fig = px.density_mapbox(incidents,
                            lat="lat",
                            lon="lng",
                            hover_name="shortDesc",
                            hover_data=['id', 'type', 'endDatetime'],
                            mapbox_style="open-street-map",
                            **kwargs)
    
    # Adjusts margin and adds layer for county boundaries
    fig.update_layout(
        margin={"r":0, "l":0,"b":0,"t":0},
        mapbox_layers=[
            {
                "source": json.loads(cagdf.geometry.to_json()),
                "below": "traces",
                "type": "line",
                "color": "black",
                "line": {"width": 1},
            },
        ],
        mapbox_style="open-street-map",
    )
    return fig