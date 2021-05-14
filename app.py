import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import flask
import pandas as pd
import plotly.express as px
from dash.dependencies import Input, Output
import Metodos as st


def obtenernombre(appid):
    nombre = data_steam[data_steam['appid'] == int(appid)]['name']
    for val in nombre:
        return val


def obtenerjugadores(appid, estado):
    if estado == 1:
        arreglo_nombre.clear()
        arreglo_jugadores.clear()

        key = os.environ['TOKEN']
        url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?key=" + key

        parametros = {"appid": appid}
        # Se obtiene el JSON como respuesta de la API de steamspy
        respuesta_json = st.obtener_respuestaJSON(url, parameters=parametros)

        # Se construye el dataframe base con el archivo JSON
        df_steamspy = pd.DataFrame.from_dict(respuesta_json, orient='index')

        arreglo_jugadores.append(int(df_steamspy.iloc[0]['player_count']))
        arreglo_nombre.append(obtenernombre(appid))

        df = pd.DataFrame({'juegos': arreglo_nombre,
                           'jugadores': arreglo_jugadores})

        return df
    else:
        key = os.environ['TOKEN']
        url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?key=" + key

        parametros = {"appid": appid}
        # Se obtiene el JSON como respuesta de la API de steamspy
        respuesta_json = st.obtener_respuestaJSON(url, parameters=parametros)

        # Se construye el dataframe base con el archivo JSON
        df_steamspy = pd.DataFrame.from_dict(respuesta_json, orient='index')

        arreglo_jugadores.append(int(df_steamspy.iloc[0]['player_count']))
        arreglo_nombre.append(obtenernombre(appid))

        df = pd.DataFrame({'juegos': arreglo_nombre,
                           'jugadores': arreglo_jugadores})

        return df


data_steam = pd.read_csv("steamspy_data.csv")

arreglo_nombre = []
arreglo_jugadores = []

appid = data_steam['appid']
name = data_steam['name']

external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
                "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]

server = flask.Flask(__name__) # define flask app.server
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, server=server) # call flask server
app.title = "Steam Player Analytics: Know the game"

# ------------------------------LAYOUT---------------------------------------------
app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H1(
                    children="Gamelytics", className="header-title"
                ),
                html.P(
                    children="En base a videojuegos conocidos te mostraremos la cantidad de jugadores en tiempo real "
                             "para que puedas ver si es viable o no para comprar el videojuego online.",
                    className="header-description",
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Dropdown(
                        id='dropdown',
                        options=[
                            {'label': name[x], 'value': appid[x]} for x in range(len(appid) - 1)
                        ],
                        value=appid[0],
                        placeholder="Selecciona un juego",
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(id="bar-chart"),
                    className="card",
                ),
                html.Div(
                    children=html.Button('Reiniciar Tabla', id='submit-val', n_clicks=0),
                    className="button",
                ),
            ],
            className="wrapper",
        ),

    ])


# ------------------------------LAYOUT---------------------------------------------

@app.callback(
    Output("bar-chart", "figure"),
    Input("dropdown", "value"),
    Input('submit-val', 'n_clicks'))
def update_bar_chart(value, n_clicks):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    valor = str(value)
    val = 0

    if 'submit-val' in changed_id:
        val = 1
        valorTemp = 10
        fig1 = px.bar(obtenerjugadores(valorTemp, val), x='juegos', y='jugadores')
    else:
        fig1 = px.bar(obtenerjugadores(valor, val), x='juegos', y='jugadores')

    return fig1