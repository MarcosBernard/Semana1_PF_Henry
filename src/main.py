import requests
import header as h
import pandas as pd
import os
from os import listdir
from os.path import isfile, join


from google.cloud import storage

# Descargar archivos de una url
for i in range(3):
    URL = h.urls[i]
    response = requests.get(URL)
    open("./Datasets/Descarga/{}.json.gz".format(h.filenames[i]), "wb").write(response.content)

# Cargando archivos descargado en fragmentos
for i in range(3):
    h.getChunkDF('./Datasets/Descarga/','{}.json.gz'.format(h.filenames[i]),500000,type = '.csv')



# Listando archivos a cargar
onlyfiles = [f for f in listdir('./Datasets/CSVs/') if isfile(join('./Datasets/CSVs/', f))]

# Reemplazamos nulos en cada tabla
for i in range(len(onlyfiles)):
    if onlyfiles[i] != '.gitkeep':
        df = h.ReemplazarNulos('./Datasets/CSVs/',onlyfiles[i])
        df.to_csv('./Datasets/ETL/{}'.format(onlyfiles[i]),index=False)

# Listando archivos a cargar
onlyfiles = [f for f in listdir('./Datasets/ETL/') if isfile(join('./Datasets/ETL/', f))]

# Separamos columna helpful y creamos una columna Fecha
for i in range(len(onlyfiles)):
    if onlyfiles[i] != '.gitkeep':
        df = h.helpful_Fecha('./Datasets/ETL/',onlyfiles[i])
        df.to_csv('./Datasets/ETL/{}'.format(onlyfiles[i]),index=False)

# Cargando archivos a GCS
# Create a new bucket
h.create_bucket('bucket-pf-henry')

# Accessing a specific bucket
my_bucket = h.storage_client.get_bucket('bucket-pf-henry')

# Listando archivos a cargar
from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir('./Datasets/ETL/') if isfile(join('./Datasets/ETL/', f))]

# Subiendo archivos
for i in range(len(onlyfiles)):
    if onlyfiles[i] != '.gitkeep':
        h.upload_to_bucket('{}'.format(onlyfiles[i]),'./Datasets/ETL/{}'.format(onlyfiles[i]),'bucket-pf-henry')
        print('Archivo {}'.format(onlyfiles[i]), 'subido!')