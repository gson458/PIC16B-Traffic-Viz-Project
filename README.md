WARNING: API keys have been provided in this repo in the credentials.py files. These may not work in the future; if so just create you own Mapquest and Google keys and replace the variables accordingly.


The finalized code is all contained within the 'Web App V2' folder. Other files related to data storage are in the 'webapp_county_data' or the base folder.


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


To start the program, go to the app.py file in the Web App V2 folder. Run the file in whatever IDE or terminal you are using.

Note that on first startup, if the county data files in webapp_county_data do not already exist, initializing the database and importing county boundary data may take a minute or two (depending on the speed of your computer).

Once the web app has started, the terminal should print a line saying something like:

"Running on http://127.0.0.1:####" where #### is a four-digit number.

Open the link it gives in your preferred browser.

Once the page is open, type in a California county into the textbox labeled 'County'. If this is the first time running the program or checking the inputted county, click the 'Update county incidents' button.

NOTE: Do not include the word 'county' when typing in a county name. For example, instead of 'orange county' just type 'orange'.

If your inputted county is valid, a scattermap should be displayed with all the current incidents in the given county, along with any past incidents previously stored in your database. If nothing shows up and the message in the top left corner says 'There are 0 incidents in the area', then there may be no traffic incidents in that county (somehow).

To update the database and figure with any incidents up to the current time, just click the 'Update county incidents' button again.

If you want to switch between map modes (scatter mapbox and heatmap), click one of the two options in the dropdown menu under 'Map Type'.

Below the line saying 'Incidents' should be a table of all incidents displayed on the map. Click on the arrows at the bottom right to scroll through the pages of the table.


ANOTHER NOTE: Severity is a measure of how much an incident affects traffic; 1 is mild, 2 is a notable slowdown, 3 is usually a full road closure


There you go! Now you can see all the real-time traffic incidents in California!