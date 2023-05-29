# Import Libraries
import dash
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc


app = Dash(__name__,external_stylesheets=[dbc.themes.COSMO], use_pages=True)

# Declare server for Heroku deployment. Needed for Procfile.
server = app.server


app.layout = dbc.Container([
    #Framework of the main app
    html.Hr(),
    dbc.Stack(
        [
        html.Div(
            dcc.Link("Search by " + page['name'], href=page['path']),
            )
        for page in dash.page_registry.values()
        ],
              direction = "horizontal",
              gap = 4,),
    html.Hr(),
    
    dash.page_container
    ])

if __name__ == '__main__':
    app.run_server(debug=True)