from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from pymongo import MongoClient
import pandas as pd

# Connessione a MongoDB
mongo_uri = "mongodb://mongoadmin:secret@localhost:27017/"
client = MongoClient(mongo_uri)
db = client['sales_data']
# Utilizzo di due collezioni differenti
collection_sales_data = db['sales_data']
collection_aggregated_hourly_sales_per_station = db['aggregated_hourly_sales_per_station']

# Inizializzazione dell'app Dash
app = Dash(__name__)


#------------------

# Layout dell'app con entrambi i componenti Graph per i grafici
app.layout = html.Div([
    html.Div([
        #dcc.Graph(id='live-update-graph', style={'height': '500px'}),
        dcc.Graph(id='live-update-graph', style={'height': '470px'}),
        dcc.Interval(
            id='interval-component-live',
            interval=3000,  # Aggiornamento ogni 3 secondi
            n_intervals=0
        )
    ], style={'width': '50%', 'float': 'left'}),
    


      # Aggiunto componente Graph per il grafico a torta
    html.Div([
        dcc.Graph(id='pie-chart'),
    ], style={'width': '50%', 'display': 'inline-block'}),
    


    html.Div([
        dcc.Graph(id='line-chart'),
        dcc.Interval(
            id='interval-component-line',
            interval=30000,  # Aggiornamento ogni 30 secondi
            n_intervals=0
        )
    ], style={'width': '100%', 'clear': 'both'})


])

#----------------------------------

# Callback per aggiornare il primo grafico (grafico a barre)
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component-live', 'n_intervals')])
def update_graph_live(n):
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$metadata.station_id",
            "records": {"$push": "$$ROOT"}
        }},
        {"$project": {
            "last_records": {"$slice": ["$records", 50]}
        }},
        {"$addFields": {
            "first": {"$arrayElemAt": ["$last_records.total_bikes_available", -1]},
            "last": {"$arrayElemAt": ["$last_records.total_bikes_available", 0]},
            "station_name": {"$arrayElemAt": ["$last_records.metadata.name", 0]}
        }},
        {"$match": {"station_name": {"$ne": ""}, "station_name": {"$exists": True}, "station_name": {"$type": "string"}}},  # Escludere stazioni con nome vuoto o mancante
        {"$addFields": {
            "difference": {"$subtract": ["$last", "$first"]},
            "absolute_difference": {"$abs": {"$subtract": ["$last", "$first"]}}
        }},
        {"$sort": {"absolute_difference": -1}},
        {"$limit": 20}
    ]
    aggregated_data = collection_sales_data.aggregate(pipeline)

    data = []
    for doc in aggregated_data:
        adjusted_difference = doc['difference'] + 1 if doc['difference'] >= 0 else doc['difference'] - 1
        data.append({
            'Station Name': doc['station_name'],
            'Adjusted Bikes Available Change': adjusted_difference,
            'First Record Timestamp': doc['last_records'][-1]['timestamp'],
            'Last Record Timestamp': doc['last_records'][0]['timestamp']
        })

    df = pd.DataFrame(data)
    df = df.sort_values('Station Name')

    fig = px.bar(df, x='Station Name', y='Adjusted Bikes Available Change',
                 title="Top 20 Stations by activity (near-real-time values)",
                 labels={"Adjusted Bikes Available Change": "Bikes Available"},
                 color='Adjusted Bikes Available Change',
                 hover_data=['First Record Timestamp', 'Last Record Timestamp'],
                 color_continuous_scale=px.colors.sequential.Viridis,
                 range_y=[-10, 10])

    fig.update_traces(marker_line_width=1.5, opacity=0.6)
    fig.update_layout(width=800, xaxis_tickangle=-45, xaxis_tickfont=dict(size=10))

    return fig



# Callback per il grafico a torta
@app.callback(Output('pie-chart', 'figure'),
              [Input('interval-component-line', 'n_intervals')])
def update_pie_chart(n):
    aggregation_pipeline = [
        {"$group": {
            "_id": "$metadata.name",
            "total_partenze": {"$sum": "$departures"}
        }},
        {"$sort": {"total_partenze": -1}},
        {"$limit": 10}
    ]
    top_stations = list(db['sales_data'].aggregate(aggregation_pipeline))
    df = pd.DataFrame(top_stations)
    
    if not df.empty:
        fig = px.pie(df, names='_id', values='total_partenze', title='Top 10 Stations by total departures')
    else:
        fig = px.pie(title="Nessun dato disponibile")
    
    return fig





# Callback per aggiornare il secondo grafico (grafico a linee)
@app.callback(Output('line-chart', 'figure'),
              [Input('interval-component-line', 'n_intervals')])
def update_chart(n):
    most_recent_date_doc = collection_aggregated_hourly_sales_per_station.find_one(sort=[("date", -1)])
    if most_recent_date_doc:
        most_recent_date = most_recent_date_doc["date"]
    else:
        return px.line(title="Nessun dato disponibile")

    aggregation_pipeline = [
        {"$match": {"date": most_recent_date}},
        {"$group": {
            "_id": "$station_name",
            "total_partenze": {"$sum": "$cnt_partenze"}
        }},
        {"$sort": {"total_partenze": -1}},
        {"$limit": 20}
    ]
    top_stations = list(collection_aggregated_hourly_sales_per_station.aggregate(aggregation_pipeline))
    top_station_names = [station['_id'] for station in top_stations]

    filtered_documents = collection_aggregated_hourly_sales_per_station.find({
        "date": most_recent_date,
        "station_name": {"$in": top_station_names}
    }).sort([("station_name", 1), ("ora", 1)])

    df = pd.DataFrame(list(filtered_documents))
    if df.empty:
        return px.line(title="Nessun dato disponibile")

    df.rename(columns={'ora': 'Ora', 'station_name': 'Nome Stazione', 'cnt_partenze': 'Cnt Partenze', 'date': 'Date'}, inplace=True)

    fig = px.line(df, x='Ora', y='Cnt Partenze', color='Nome Stazione',
                  title=f"Top 20 Stations by Hourly Departures - Date: {most_recent_date}",
                  labels={"Cnt Partenze": "Number of Departures", "Ora": "Hour of the Day", "Nome Stazione": "Station Name"})

    #fig.update_layout(width=1400, height=400)

    return fig

# Avvia il server
if __name__ == '__main__':
    app.run_server(debug=True, port=8090)  # Porta modificata per evitare conflitti