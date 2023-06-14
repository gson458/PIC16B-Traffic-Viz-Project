WARNING: API keys have been provided in this repo in the credentials.py files. These may not work in the future; if so just create your own Mapquest and Google keys and replace the variables accordingly.


The finalized code is all contained within the 'Web App V2' folder. Other files related to data storage are in the 'webapp_county_data' or the base folder.


The goal of our project is to create a website that presents a map of real-time, interactive traffic incident information and location in California. Similar to how morning news programs have a traffic segment with a host describing incidents that might affect a commuter’s trip to work, our program will display real-time traffic incidents on a map with the same information that a news host might give you. For example, a road closure blocking access to one route or a traffic accident causing a slowdown will be shown as they happen so a user can stay informed of road conditions.

Our program pulls traffic incident information from the Mapquest API, then uses each set of coordinates to add address information from Google’s reverse geocoding API. These are stored in a SQL database to prevent constant API calls. A person using the webapp can trigger an update to the database and subsequently retrieve the incidents in a given county (in California), which are then displayed in an interactive plotly figure containing all of the relevant incident info in the figure and a table below.


Follow the instructions below to use the program.





To start using the traffic visualization, you need the following packages:
    dash
    dash_bootstrap_components
    requests
    sqlite3
    asyncio
    aiohttp
    pandas
    plotly
    nest_asyncio
    geopandas

Install with conda or import the environment using the environment.yml file in the terminal with the following command:
conda env create -n trafficviz --file environment.yml


To start the program, go to the app.py file in the 'Web App V2' folder. Run the file in whatever IDE or terminal you are using.

Note that on first startup, if the county data files in 'webapp_county_data' do not already exist, initializing the database and importing county boundary data may take a minute or two (depending on the speed of your computer).

Once the web app has started, the terminal should print a line saying something like:

"Running on http://127.0.0.1:####" where #### is a four-digit number.

Open the link it gives in your preferred browser.

Once the page is open, type in a California county into the textbox labeled 'County'. If this is the first time running the program or checking the inputted county, click the 'Update county incidents' button.

NOTE: Do not include the word 'county' when typing in a county name. For example, instead of 'orange county' just type 'orange'.

If your inputted county is valid, a scattermap should be displayed with all the current incidents in the given county, along with any past incidents previously stored in your database. If nothing shows up and the message in the top left corner says 'There are 0 incidents in the area' even after clicking the 'Update county incidents' button, then there may be no traffic incidents in that county (somehow).

To update the database and figure with any incidents up to the current time, just click the 'Update county incidents' button again.

If you want to switch between map modes (scatter mapbox and heatmap), click one of the two options in the dropdown menu under 'Map Type'.

Below the line saying 'Incidents' should be a table of all incidents displayed on the map. Click on the arrows at the bottom right to scroll through the pages of the table.


ANOTHER NOTE: Severity is a measure of how much an incident affects traffic; 1 is mild, 2 is a notable slowdown, 3 is usually a full road closure.


There you go! Now you can see all the real-time traffic incidents in California!