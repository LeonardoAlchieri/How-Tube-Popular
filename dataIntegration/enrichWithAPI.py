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

# This one enriches scrape collections with regionCode from API

def main():

    comm = MPI.COMM_WORLD

    ID = comm.Get_rank()


    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]

    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    collectionName = "scrape"
    scraperCollection = databaseMongo[collectionName]

    collectionName = "API"
    APICollection = databaseMongo[collectionName]

    collectionName = "testEnrich"
    resultCollection = databaseMongo[collectionName]
    print("OK0")
    BATCH_SIZE = round(APICollection.count_documents({})/comm.Get_size() + 0.5)
    cursorAPI = APICollection.find().skip(BATCH_SIZE*ID).limit(BATCH_SIZE)

    BATCH_SIZE = round(APICollection.count_documents({})/comm.Get_size() + 0.5)
    cursorScrape = scraperCollection.find().skip(BATCH_SIZE*ID).limit(BATCH_SIZE)
    print("OK1")
    upserts = [ UpdateOne(
        {'id':APIDoc["id"]},
        {# If there a duplicate, the scrapign will not be done.
            '$setOnInsert': scraper,
            '$set': {"regionCode": APIDoc["regionCode"]}
        }, upsert=True) for APIDoc, scraper in zip(cursorAPI, cursorScrape)]
    print("OK2")
    resultCollection.bulk_write(upserts)
    print("OK3")

if __name__ == "__main__":
    main()
