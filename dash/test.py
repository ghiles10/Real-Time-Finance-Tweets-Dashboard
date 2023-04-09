import dash
import sys
sys.path.append(r'/workspaces/Finance-dashbord')
from dash import dcc, html , Output, Input
import dash.dependencies as dd
from dash import html
import plotly.express as px
import pandas as pd
from title_big_query import symbols_list 
from search_tweet import search_tweet



app = dash.Dash()

title_options = symbols_list()

app.layout = html.Div([
    html.H1('Cryptocurrency Dashboard'),
    html.P("""this application allows to display information and sentiment analysis"""),
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
    ])
])

@app.callback(
    dd.Output('tweets-display', 'children'),
    [dd.Input('title-dropdown', 'value')]
)
def update_tweets(selected_title):
    tweets = search_tweet(selected_title)
    return tweets

if __name__ == '__main__':
    app.run_server(debug=True)