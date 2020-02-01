import pandas as pd
import pymongo
import json
import re
import base64
from sqlalchemy import create_engine
import pymysql
from pymongo import UpdateOne
from pymongo import UpdateMany
from mpi4py import MPI
import sys
import logging
import bcolors
import os
import shutil
from apiclient.discovery import build
from datetime import datetime
import yaml
import time



def main():
    START = time.time()
    comm = MPI.COMM_WORLD

    ID = comm.Get_rank()

    logging.basicConfig(filename='./logs/log_enrichScraperWithkaggle'+str(ID)+'.log', level=logging.INFO)
    logging.info("\n")
    logging.info("Log file created. Program started.")
    logging.info("Reading config files.")




    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    LOADING_PERC = cfgMongo["percentage"]

    logging.info("Config files succesfully read.")
    logging.info("Loading Mongo collections.")
    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]

    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    collectionName = "scrape"+str(LOADING_PERC)
    scraperCollection = databaseMongo[collectionName]

    collectionName = "kaggleNation"+str(LOADING_PERC)
    kaggleCollection = databaseMongo[collectionName]

    logging.info("Mongo collections loaded.")

    BATCH_SIZE = round(kaggleCollection.count_documents({})/comm.Get_size() + 0.5)
    cursorKaggle = kaggleCollection.find().skip(BATCH_SIZE*ID).limit(BATCH_SIZE)

    logging.info("Preparing to update.")
    # This Updated enriches scraper documents with data from kaggle
    upserts = [ UpdateMany(
        {'id': kaggleDoc["id"]},
        {
            '$set': {"date": kaggleDoc["date"]}
        }) for kaggleDoc in cursorKaggle]
    logging.info("Updating documents.")
    scraperCollection.bulk_write(upserts)
    logging.info("Data saved succesfully to Mongo.")

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
    with open("results/resultTogether.csv", "a") as file:
        df.to_csv(file, header=False)


if __name__ == "__main__":
    main()
