import gzip
import os
from google.cloud import storage
import pandas as pd

filenames = [
        'MusicalInstruments',
        'Automotive',
        'PatioLawnAndGard',
        'AmazonInstantVideo',
        'Office',
        'DigitalMusic',
        'PetSupplies',
        'GroceryAndGourmetFood',
        'ToolsAndHomeImprovement',
        'Baby',
        'ToysAndGames',
        'Beauty',
        'CellphonesAndAccessories',
        'ClothingAndShoesAndJewerly',
        'SportsAndOutdoors',
        'HealthAndPersonalCare',
        'AndroidApps',
        'VideoGames',
        'HomeAndKitchen',
        'KindleStore',
        'CDAndVinyl',
        'Electronics',
        'MoviesAndTV',
        'Books',
        'Metadata'
                ]

urls =  [
        #Musical Instruments
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Musical_Instruments_5.json.gz',
        #Automotive
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Automotive_5.json.gz',
        #Patio, Lawn and Garden
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Patio_Lawn_and_Garden_5.json.gz',
        #Amazon Instant Video
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video_5.json.gz',
        #Office
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Office_Products_5.json.gz',
        #Digital Music
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Digital_Music_5.json.gz',
        #Pet Supplies
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Pet_Supplies_5.json.gz',
        #Grocery and Gourmet Food
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Grocery_and_Gourmet_Food_5.json.gz',
        #Tools and Home Improvement
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Tools_and_Home_Improvement_5.json.gz',
        #Baby
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Baby_5.json.gz',
        #Toys and Games
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Toys_and_Games_5.json.gz',
        #Beauty
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Beauty_5.json.gz',
        #Cell Phones and Accesories
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Cell_Phones_and_Accessories_5.json.gz',
        #Clothing, Shoes and Jewelry
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Clothing_Shoes_and_Jewelry_5.json.gz',
        #Sports and Outdoors
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Sports_and_Outdoors_5.json.gz',
        #Health and Personal Care
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Health_and_Personal_Care_5.json.gz',
        #Apps for Android
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Apps_for_Android_5.json.gz',
        #Video Games
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Video_Games_5.json.gz',
        #Home and Kitchen
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Home_and_Kitchen_5.json.gz',
        #Kindle Store
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Kindle_Store_5.json.gz',
        #CD and Vinyl
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_CDs_and_Vinyl_5.json.gz',
        #Electronics
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Electronics_5.json.gz', 
        #Movies and TV
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Movies_and_TV_5.json.gz',       
        #Books
        'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Books_5.json.gz',
        #Metadata
        'http://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz'
                ]

def parse(path):
    with gzip.open(path, 'rb') as g:
        for l in g:
            yield eval(l)

def save_partition(filename,df,chunksnum,type):
    dfout = pd.DataFrame.from_dict(df, orient='index')
    if type == '.csv':
        dfout.to_csv('./Datasets/CSVs/{}_{}{}'.format(filename.replace('.json.gz',''),chunksnum,type))
    else: 
        dfout.to_json('./{}_{}{}'.format(filename.replace('.json.gz',''),chunksnum,'.json'))
    return(True)

def getChunkDF(filepath,filename,chunklen,type='.csv'):
  '''
  Parámetros: 
  filepath: ruta de origen del archivo .gz
  filename: nombre del archivo en formato .gz
  chunklen: número de filas del fragmento
  type: tipo de archivo, .csv por defecto
  '''
  path = os.path.join(filepath,filename)
  i = 0
  df = {}
  chunksnum = 1
  for d in parse(path):
    df[i] = d
    i += 1
    if i == chunklen:
      save_partition(filename,df,chunksnum,type)
      df = {}; chunksnum += 1; i = 0 # Reinicia variables
  if i != 0: save_partition(filename,df,chunksnum,type)
  return True



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './Credenciales/GCS.json'
storage_client = storage.Client()

def create_bucket(bucket_name):
    bucket_name = 'bucket-pf-henry'
    bucket = storage_client.bucket(bucket_name)
    bucket.location = 'US'
    try:
        bucket = storage_client.create_bucket(bucket)
    except Exception as e:
        print (e)


def upload_to_bucket(blob_name,file_path,bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return(True)
    except Exception as e:
        print (e)
        return False