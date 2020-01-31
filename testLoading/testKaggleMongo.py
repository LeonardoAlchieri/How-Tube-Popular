import pandas as pd
import pymongo
import json
import re
import base64
from sqlalchemy import create_engine
import pymysql
from pymongo import UpdateOne
from mpi4py import MPI
import sys
import logging
import bcolors
import os
import shutil
from pprint import pprint
import yaml
import time

def clean_ref_date(x):
    year = "20"+x[:2]
    day = x[3:5]
    month = x[-2:]
    return str(year)+"-"+str(month)+"-"+str(day)

def main():
    START = time.time()
    prepare_logging()

    try:
        LINE_START = sys.argv[1]
    except:
        LINE_START = 0

    # Start parallel session
    comm = MPI.COMM_WORLD

    ID = comm.Get_rank() + LINE_START


    logging.basicConfig(filename='./logs/log_Kaggle'+str(ID)+'.log', level=logging.INFO)

    # Load MySQL
    PASSWORD = base64.b64decode('Q2FjY2FGcml0dGE2OQ==').decode("utf-8")
    db_connection_str = 'mysql+pymysql://giuseppe:'+str(PASSWORD)+'@tars1.bounceme.net:27050/videos'
    connectorSQL = create_engine(db_connection_str)

    # Load MongoDB
    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    LOADING_PERC = cfgMongo["percentage"]

    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]
    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    country_list = ["CAvideos", "USvideos", "GBvideos"]
    #collectionName = 'kaggle'+str(country_list[ID][:2])
    collectionName = "kaggleNation"+str(LOADING_PERC)
    collectionMongo = databaseMongo[collectionName]
    collectionMongo.create_index([ ('id', 1), ('ref_date', 1) ])
    clientMongo.admin.command("shardCollection", str(MONGO_DATABASE)+"."+str(collectionName), key={"id":1, "ref_date":1})
    logging.info("["+str(ID)+"] Loading data from SQL.")
    #
    #   Potremmo parallelizzare sui chunk, in cui un primario tiene la memoria
    #   e gli altri prendono da quello.
    #
    chunksSQL = pd.read_sql('''SELECT TOP '''+str(LOADING_PERC)+''' PERCENT video_id, trending_date, title, channel_title,
                            publish_time, tags, views, likes, dislikes, comment_count, thumbnail_link,
                            comments_disabled, ratings_disabled, video_error_or_removed,
                            description  FROM '''+str(country_list[ID]),con=connectorSQL, chunksize=2000)

    logging.info("["+str(ID)+"] Data loaded from SQL.")
    count = 0
    for chunk in chunksSQL:
        logging.info("["+str(ID)+"] Step "+str(count))
        # Change column names to fit our naming schema
        chunk = chunk.rename(columns={"trending_date": "ref_date", "video_id": "id",
                            "channel_title": "channel_name", "publish_time": "date",
                            "comment_count": "comments count"})
        # Change date
        chunk["date"] = chunk["date"].apply(lambda x: x[:10])
        # Change ref_date
        chunk["ref_date"] = chunk["ref_date"].apply(clean_ref_date)
        chunk = chunk.to_dict("records")

        # Add regionCode & Kaggle
        upserts = [ UpdateOne(
            {'id':x['id'], 'ref_date':x['ref_date']},
            {
                '$setOnInsert':x,
                '$addToSet': {"regionCode": str(country_list[ID][:2])}
            }, upsert=True) for x in chunk]


        collectionMongo.bulk_write(upserts)


        count = count + 1
    logging.info("["+str(ID)+"] Program completed.")
    END = time.time()
    time_result = {
        "collection": collectionName,
        "percentage": LOADING_PERC,
        "time": (END - START)
    }
    with open("results/"+str(collectionName)+str(LOADING_PERC)+".json", "w") as file:
        json.dump(time_result, file)

if __name__ == "__main__":
    main()
