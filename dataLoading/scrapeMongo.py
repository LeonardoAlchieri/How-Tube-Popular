# by Leonardo Alchieri, 2019

from YoutubeScraper import Scraper
import bcolors
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import os
import json
import re
from mpi4py import MPI
import logging
import shutil
from datetime import date
import sys
import yaml
import re
import base64
import pymongo
from pymongo import UpdateOne
import os
import shutil



def prepare_logging():

    # define the name of the directory to be created
    path = "logs"

    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)

    try:
        os.mkdir(path)
    except OSError:
        print (bcolors.WARN+"Creation of the directory "+str(path)+" failed"+bcolors.END)
    else:
        print ("Successfully created the directory %s " % path)


def main():

#    prepare_logging()

    #try:
    WHICH_COLLECTION = int(sys.argv[1])

    #except:
    #    print(bcolors.ERR+"[USAGE] scrapeMongo.py [1-Kaggle, 2-API]"+bcolors.END)
    #    return -1


    # Start parallel session
    comm = MPI.COMM_WORLD

    ID = comm.Get_rank()

    logging.basicConfig(filename='./logs/log_Scraping'+str(ID)+'.log', level=logging.INFO)

    # Connect to mongo database
    logging.info("["+str(ID)+"] Connecting to Mongo.")
    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]
    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]

    # Connect to source collection
    if WHICH_COLLECTION == 1:
        collectionName = "kaggleNation"
    elif WHICH_COLLECTION == 2:
        collectionName = "API"
    else:
        logging.error("["+str(ID)+"] Couldn't figure out which database. Use 1 for Kaggle, 2 for API.")
        return -1

    sourceCollection = databaseMongo[collectionName]

    # Connect to result collection
    collectionName = "scrape"
    resultCollection = databaseMongo[collectionName]

    resultCollection.create_index([ ('id', 1), ('ref_date', 1) ])
    clientMongo.admin.command("shardCollection", str(MONGO_DATABASE)+"."+str(collectionName), key={"id":1, "ref_date":1})
    logging.info("["+str(ID)+"] Connection to Mongo collections established.")

    # Start Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--lang=en-us')
    driver = webdriver.Chrome(executable_path=os.path.abspath("./chromedriver"), options=chrome_options)
    logging.info("["+str(ID)+"] Webdriver loaded")


    BATCH_SIZE = round(sourceCollection.count_documents({})/comm.Get_size() + 0.5)
    logging.info("["+str(ID)+"] Batch size calculated.")
    cursor = sourceCollection.find().skip(BATCH_SIZE*ID).limit(BATCH_SIZE)
    logging.info("["+str(ID)+"] Scraping data.")
    upserts = [ UpdateOne(
        {'id':doc['id'], 'ref_date':str(date.today())},
        {# If there a duplicate, the scrapign will not be done.
            '$setOnInsert':Scraper(id = doc['id'], driver = driver, scrape=True, close_on_complete=False,
            speak=False).as_dict()
        }, upsert=True) for doc in cursor]
    logging.info("["+str(ID)+"] Data succesfully scraped.")
    logging.info("["+str(ID)+"] Bulk writing data on Mongo.")
    resultCollection.bulk_write(upserts)
    logging.info("["+str(ID)+"] Data saved succesfully.")

if __name__ == "__main__":
    main()
