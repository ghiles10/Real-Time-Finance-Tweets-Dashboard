from pathlib import Path
import sys

ROOT = Path(__file__).parent.parent 
sys.path.append(str(ROOT)) 

import dash
import plotly.express as px
from dash import dcc, html, Input, Output

from app_ui.services.big_query_services import BigQueryService
from app_ui.services.search_tweet import search_tweet
from config import core, schema

# Load configuration
APP_CONFIG = schema.BigQuery(**core.load_config().data["big_query"])
project_id = APP_CONFIG.project_id
table_id = APP_CONFIG.dataset_id + "." + "fact"

# Dash app
app = dash.Dash()

# Get symbols list
bq = BigQueryService(project_id)
title_options = bq.symbols_list(table_id)

# Get plot
df = bq.avg_hour_cryptos(table_id)
fig = px.bar(df, x="symbol", y="avg_difference", title="avg difference")

app.layout = html.Div([
    html.H1('Cryptocurrency Dashboard'),
    html.P("This application allows to display information and sentiment analysis"),
    html.Div([
        dcc.Dropdown(
            id='title-dropdown',
            options=[{'label': title, 'value': title} for title in title_options],
            value=title_options[0]
        ),
        html.Div([
            html.Div(["Tweet"], style={'text-align': 'center', 'font-size': '400'}),
            html.Div(id='tweets-display', children=[])
        ])
    ]),
    dcc.Graph(figure=fig)
])


@app.callback(
    Output('tweets-display', 'children'),
    [Input('title-dropdown', 'value')]
)
def update_tweets(selected_title):
    tweets = search_tweet(selected_title)
    return tweets


if __name__ == '__main__':
    app.run_server(debug=True)
