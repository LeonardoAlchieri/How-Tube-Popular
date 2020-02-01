import pandas as pd
import pymongo
import json
import re
import base64
from sqlalchemy import create_engine
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
from textblob import TextBlob
from PIL import Image
import io
import numpy as np
import scipy
import scipy.misc
import scipy.cluster
from pprint import pprint
import requests

def get_thumbnail_hq(diz):
    try:
        url = "https://i.ytimg.com/vi/"+diz["id"]+"/hqdefault.jpg"
        response = requests.get(url)
        base64_bytes = base64.b64encode(response.content)
        base64_string = base64_bytes.decode("utf-8")
        return base64_string
    except:
        return "Unknown"

def main():
    try:
        LINE_START = sys.argv[1]
    except:
        LINE_START = 0

    comm = MPI.COMM_WORLD

    ID = comm.Get_rank() + int(LINE_START)

    logging.basicConfig(filename='./logs/log_enrichThumbnail'+str(ID)+'.log', level=logging.INFO)
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

    logging.info("Mongo collection loaded.")

    BATCH_SIZE = round(scraperCollection.count_documents({})/comm.Get_size() + 0.5)
    # I set no timeout because the downloading of thumbnails is quite heavy
    cursorScraper = scraperCollection.find(no_cursor_timeout=True).skip(BATCH_SIZE*ID).limit(BATCH_SIZE)

    logging.info("Prapering to update.")
    upserts = [ UpdateOne(
        {'id':scraperDoc["id"], "ref_date": scraperDoc["ref_date"]},
        {
            '$set': {"thumbnail_as_bytes": get_thumbnail_hq(scraperDoc)}
        }) for scraperDoc in cursorScraper]
    logging.info("Updating documents.")
    try:
        scraperCollection.bulk_write(upserts, ordered=False)
    except pymongo.errors.BulkWriteError as bwe:
        pprint(bwe.details[0])
        raise

    cursorScraper.close()
    logging.info("Data saved succesfully to Mongo and cursor closed.")


if __name__ == "__main__":
    main()
