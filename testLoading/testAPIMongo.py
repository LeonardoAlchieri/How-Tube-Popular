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
import pandas as pd

def main():
    START = time.time()

    ID = 0
    logging.basicConfig(filename='./logs/log_API'+str(ID)+'.log', level=logging.INFO)
    logging.info("\n")


    # Load MongoDB
    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    LOADING_PERC = cfgMongo["percentage"]

    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]
    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    #collectionName = 'kaggle'+str(country_list[ID][:2])
    collectionName = "API"+str(LOADING_PERC)
    collectionMongo = databaseMongo[collectionName]
    collectionMongo.create_index([ ('id', 'hashed') ])
    clientMongo.admin.command("shardCollection", str(MONGO_DATABASE)+"."+str(collectionName), key={"id":'hashed'})
    logging.info("["+str(ID)+"] Loading API data.")

    with open("dataToLoad/APIResults.csv", "r") as file:
        APIData = pd.read_csv(file)


    NUMBER_OF_ROWS = len(APIData)

    LIMIT = int(NUMBER_OF_ROWS * LOADING_PERC * 0.01)
    logging.info("["+str(ID)+"] Limiting to "+str(LIMIT)+" rows out of "+str(NUMBER_OF_ROWS)+".")

    BATCH_SIZE = round(LIMIT/comm.Get_size() + 0.5)
    if(ID == 0):
        APIData = APIData[:BATCH_SIZE]
    elif(ID == (comm.Get_size()-1)):
        APIData = APIData[BATCH_SIZE*ID:]
    else:
        APIData = APIData[BATCH_SIZE*ID:BATCH_SIZE*(ID+1)]
    #
    #   Potremmo parallelizzare sui chunk, in cui un primario tiene la memoria
    #   e gli altri prendono da quello.
    #
    logging.info("["+str(ID)+"] Data API loaded.")

    upserts = [ UpdateOne(
            {'id':x},
            {
                '$setOnInsert':{"id":x},
                '$addToSet': {"regionCode": "US"}
            }, upsert=True) for x in APIData["0"]]

    collectionMongo.bulk_write(upserts)

    logging.info("["+str(ID)+"] Program completed.")
    END = time.time()
    time_result = {
        "collection": collectionName,
        "percentage": LOADING_PERC,
        "time": (END - START),
        "number of cores": 1
    }
    df = pd.DataFrame()
    df = df.append(time_result, ignore_index=True)
    with open("results/kaggleNation.csv", "a") as file:
        df.to_csv(file, header=False)


if __name__ == "__main__":
    main()
