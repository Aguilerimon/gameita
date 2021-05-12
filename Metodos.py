import json
import csv
import datetime as dt
import os
import statistics
import time
from ssl import SSLError

import numpy as np
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


def resetea_indice(ruta_descarga, nombre_indice):
    # Obtiene la ruta del archivo de indices
    ruta = os.path.join(ruta_descarga, nombre_indice)

    # Abre el archivo de indice en un modo de escritura y añade un 0 al inicio como reset
    with open(ruta, 'w') as f:
        print(0, file=f)


def obtener_indice(ruta_descarga, nombre_indice):
    try:
        # Obtiene la ruta del archivo de indices
        ruta = os.path.join(ruta_descarga, nombre_indice)

        # Abre el archivo de indice en un modo de lectura y obtiene el indice en una variable entera
        with open(ruta, 'r') as f:
            indice = int(f.readline())

    # Si no existe el archivo de indice retorna un 0
    except FileNotFoundError:
        indice = 0

    return indice


def preparar_csv(ruta_descarga, nombre_csv, indice, columnas):
    if indice == 0:
        # Obtiene la ruta del csv
        ruta = os.path.join(ruta_descarga, nombre_csv)

        # Abre el csv en un modo de escritura y añade las columnas como cabeceras
        with open(ruta, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columnas)
            writer.writeheader()


# Método que convierte
def obtener_datos_juegos(inicio, detenido, analizador, pausa, lista_app):
    datos_juegos = []

    for index, row in lista_app[inicio:detenido].iterrows():
        print('Current index: {}'.format(index), end='\r')
        appid = row['appid']
        name = row['name']
        price = row['price']

        datos = analizador(appid)
        datos_juegos.append(datos)

        time.sleep(pausa)

    return datos_juegos


def procesamiento_lotes(analizador, lista_app, ruta_descarga, nombre_csv, nombre_indice,
                        columnas, inicio, final, tamaño_lote, pausa):
    print('Iniciando en el indice {}: \n'.format(inicio))

    if final == -1:
        final = len(lista_app) + 1

    # Crea un arreglo que contiene un conjunto de valores entre el valor de inicio y uno final, pudiendo definir
    # un valor de incremento.
    # array[0, 5, 10, 15]
    def_lote = np.arange(inicio, final, tamaño_lote)

    # Agrega en la ultima posicion del areglo def_lote el valor de la variable final
    # array[0, 5, 10, 15, 20]
    lotes = np.append(def_lote, final)

    # Contador de los juego añadidos
    juegos_añadidos = 0

    # Definicion del arreglo de tiempo de ejecucion
    tiempos = []

    for i in range(len(lotes) - 1):

        # Guarda en una variable la hora de ejecucion en segundos en UTC
        tiempo_inicio = time.time()

        # Toma los valores limites para generar un lote segun la iteracion del ciclo
        inicio = lotes[i]
        detenido = lotes[i + 1]

        # Se obtiene el archivo JSON con los detalles de cada juego segun los limites
        datos_juegos = obtener_datos_juegos(inicio, detenido, analizador, pausa, lista_app)

        # Obtiene la ruta del archivo csv
        ruta_csv = os.path.join(ruta_descarga, nombre_csv)

        # Añade cada detalle del juego al csv segun coincidan las columnas
        with open(ruta_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columnas, extrasaction='ignore')

            # Se realiza una espera para evitar sobrecargas
            for j in range(3, 0, -1):
                print("\rSe escribira la informacion de la lista de juegos, no detenga el script! ({})".format(j), end='')
                time.sleep(0.5)

            # Se escriben los valores del juego coincidiendo las columnas
            writer.writerows(datos_juegos)
            print('\rExportando las lineas {}-{} al archivo {}.'.format(inicio, detenido - 1, nombre_csv), end=' ')

        juegos_añadidos += len(datos_juegos)

        ruta_indice = os.path.join(ruta_descarga, nombre_indice)

        # Escribe el ultimo indice en un TXT
        with open(ruta_indice, 'w') as f:
            index = detenido
            print(index, file=f)

        # Guarda en un variable la hora de finalizacion de la tarea
        tiempo_final = time.time()

        # Calcular el tiempo total transcurrido
        tiempo_transcurrido = tiempo_final - tiempo_inicio

        # Guarda al final del arreglo de tiempo el valor del tiempo transcurrido segun el ciclo
        tiempos.append(tiempo_transcurrido)

        # Se obtiene la media aritmetica tomando los valores del arreglo de tiempos
        media_tiempo = statistics.mean(tiempos)

        # Calculo de tiempo estimado
        tiempo_estimado = (len(lotes) - i - 2) * media_tiempo

        remaining_td = dt.timedelta(seconds=round(tiempo_estimado))
        time_td = dt.timedelta(seconds=round(tiempo_transcurrido))
        mean_td = dt.timedelta(seconds=round(media_tiempo))

        # time_td(tiempo_transcurrido), mean_td(media_tiempo), remaining_td(tiempo_estimado)
        print('Lote {} Tiempo: {} (Promedio: {}, Restante: {})'.format(i, time_td, mean_td, remaining_td))

    print('\nProcesamiento de lotes completado. {} Juegos exportados'.format(juegos_añadidos))
