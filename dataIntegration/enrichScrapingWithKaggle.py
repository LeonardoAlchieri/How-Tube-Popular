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
from apiclient.discovery import build
from datetime import datetime
import yaml



def main():
    comm = MPI.COMM_WORLD

    ID = comm.Get_rank()

    logging.basicConfig(filename='./logs/log_enrichscraperWithkaggle'+str(ID)+'.log', level=logging.INFO)
    logging.info("\n")
    logging.info("Log file created. Program started.")
    logging.info("Reading config files.")




    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    logging.info("Config files succesfully read.")
    logging.info("Loading Mongo collections.")
    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]

    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    collectionName = "scrape"
    scraperCollection = databaseMongo[collectionName]

    collectionName = "kaggle"
    kaggleCollection = databaseMongo[collectionName]

    logging.info("Mongo collections loaded.")

    BATCH_SIZE = round(kaggleCollection.count_documents({})/comm.Get_size() + 0.5)
    cursorkaggle = kaggleCollection.find().skip(BATCH_SIZE*ID).limit(BATCH_SIZE)

    logging.info("Preparing to update.")
    # This Updated enriches scraper documents with data from kaggle
    upserts = [ UpdateOne(
        {'id': kaggleDoc["id"]},
        {
            '$set': {"category": kaggleDoc["category"]}
        }) for kaggleDoc in cursorkaggle]
    logging.info("Updating documents.")
    scraperCollection.bulk_write(upserts)
    logging.info("Data saved succesfully to Mongo.")


if __name__ == "__main__":
    main()
