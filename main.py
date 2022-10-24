import requests
import header as h
import pandas as pd
import os

from google.cloud import storage

# Descargar archivos de una url
for i in range(4):
    URL = h.urls[i]
    response = requests.get(URL)
    open("./Datasets/Descarga/{}.json.gz".format(h.filenames[i]), "wb").write(response.content)

# Cargando archivos descargado en fragmentos
for i in range(4):
    h.getChunkDF('./Datasets/Descarga/','{}.json.gz'.format(h.filenames[i]),500000,type = '.csv')

# Cargando archivos a GCS

# Create a new bucket
h.create_bucket('bucket-pf-henry')

# Accessing a specific bucket
my_bucket = h.storage_client.get_bucket('example_first_bucket')

# Listando archivos a cargar
from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir('./Datasets/CSVs/') if isfile(join('./Datasets/CSVs/', f))]

# Subiendo archivos
for i in range(len(onlyfiles)):
    h.upload_to_bucket('{}'.format(onlyfiles[i]),'./Datasets/CSVs/{}'.format(onlyfiles[i]),'example_first_bucket')
    print('Archivo {}'.format(onlyfiles[i]), 'subido!')