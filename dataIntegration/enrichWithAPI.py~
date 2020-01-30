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

    logging.basicConfig(filename='./logs/log_enrichAPI'+str(ID)+'.log', level=logging.INFO)
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

    collectionName = "API"
    APICollection = databaseMongo[collectionName]

    logging.info("Mongo collections loaded.")

    BATCH_SIZE = round(APICollection.count_documents({})/comm.Get_size() + 0.5)
    cursorAPI = APICollection.find().skip(BATCH_SIZE*ID).limit(BATCH_SIZE)

    logging.info("Preparing to update.")
    # This Updated enriches API documents with data from scrape
    upserts = [ UpdateOne(
        {'id':APIDoc["id"]},
        {
            '$set': {"regionCode": APIDoc["regionCode"]}
        }) for APIDoc in cursorAPI]
    logging.info("Updating documents.")
    scraperCollection.bulk_write(upserts)
    logging.info("Data saved succesfully to Mongo.")


if __name__ == "__main__":
    main()
