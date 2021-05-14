import time
from ssl import SSLError
import pandas as pd
import requests

pd.set_option("max_columns", 100)


def obtener_respuestaJSON(url, parameters=None):
    try:
        # Obtiene la respuesta de la API steamspy mediante el parametro all
        # el cual retorna todas las aplicaciones
        respuesta_api = requests.get(url=url, params=parameters)
    except SSLError as s:
        # Captura un posible error de auntenticacion SSL.
        print('SSL Error: ', s)

        # Espera 5 segundos para volver a reintentar obtener la respuesta de la API.
        # Esto evita una posible sobrecarga de peticiones.
        for i in range(5, 0, -1):
            print('\rEsperando... ({})'.format(i), end='')
            time.sleep(1)
        print('\rReintentando.' + ' ' * 10)

        # Reintento recursivo hasta que exista una correcta autenticacion SSL.
        return obtener_respuestaJSON(url, parameters)

    # Evalua si la respuesta de la API fue exitosa y la retorna en una formato JSON
    if respuesta_api:
        return respuesta_api.json()
    else:
        # Si no existe una respuesta de la API espera 10 segundos para realizar un reintento
        # recursivo hasta que exista una respuesta.
        print('No hay respuesta de steamspy, proximo reintento en 10 segundos...')
        time.sleep(10)
        print('Reintentando.')
        return obtener_respuestaJSON(url, parameters)