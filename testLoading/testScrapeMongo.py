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
from datetime import date

def main():
    START = time.time()

    try:
        LINE_START = sys.argv[1]
    except:
        LINE_START = 0

    # Start parallel session
    comm = MPI.COMM_WORLD

    ID = comm.Get_rank() + LINE_START


    logging.basicConfig(filename='./logs/log_Scrape'+str(ID)+'.log', level=logging.INFO)
    logging.info("\n")


    # Load MongoDB
    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    LOADING_PERC = cfgMongo["percentage"]

    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]
    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    collectionName = "scrape"+str(LOADING_PERC)
    collectionMongo = databaseMongo[collectionName]
    collectionMongo.create_index([ ('id', 1), ('ref_date', 1) ])
    clientMongo.admin.command("shardCollection", str(MONGO_DATABASE)+"."+str(collectionName), key={"id":1, "ref_date":1})
    logging.info("["+str(ID)+"] Loading API data.")

    with open("dataToLoad/scrapeResults.json", "r") as file:
        scrapeData = json.load(file)


    NUMBER_OF_ROWS = len(scrapeData)

    LIMIT = int(NUMBER_OF_ROWS * LOADING_PERC * 0.01)
    logging.info("["+str(ID)+"] Limiting to "+str(LIMIT)+" rows out of "+str(NUMBER_OF_ROWS)+".")

    BATCH_SIZE = round(LIMIT/comm.Get_size() + 0.5)
    if(ID == 0):
        scrapeData = scrapeData[:BATCH_SIZE]
    elif(ID == (comm.Get_size()-1)):
        scrapeData = scrapeData[BATCH_SIZE*ID:]
    else:
        scrapeData = scrapeData[BATCH_SIZE*ID:BATCH_SIZE*(ID+1)]
    #
    #   Potremmo parallelizzare sui chunk, in cui un primario tiene la memoria
    #   e gli altri prendono da quello.
    #
    logging.info("["+str(ID)+"] Data scrape loaded.")

    upserts = [ UpdateOne(
            {'id':x["id"], "ref_date": str(date.today())},
            {
                '$setOnInsert':x,
            }, upsert=True) for x in scrapeData]

    collectionMongo.bulk_write(upserts)

    logging.info("["+str(ID)+"] Program completed.")
    END = time.time()
    time_result = {
        "collection": collectionName,
        "percentage": LOADING_PERC,
        "time": (END - START),
        "number of cores": comm.Get_size()
    }
    df = pd.DataFrame()
    df = df.append(time_result, ignore_index=True)
    with open("results/kaggleNation.csv", "a") as file:
        df.to_csv(file, header=False)


if __name__ == "__main__":
    main()
