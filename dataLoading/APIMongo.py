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


def prepare_logging():

    # define the name of the directory to be created
    path = "logs"

    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)

    try:
        os.mkdir(path)
    except OSError:
        print (bcolors.ERR+"Creation of the directory"+str(path)+" failed"+bcolors.END)
    else:
        print ("Successfully created the directory %s " % path)


def set_video(key, region, PAGES, MAXIMUM, first_date, second_date, collectionMongo, youtube, categoryId):
    next_page_token = None
    for counter in range(PAGES):
        res = youtube.search().list(part = 'snippet',  # default.
                                    type = 'video',    # type cuold be video, channel or playlist.
                                    maxResults = MAXIMUM,
                                    publishedAfter = first_date,
                                    publishedBefore = second_date,
                                    pageToken = next_page_token, # Scroll the PAGES.
                                    regionCode = region,
                                    fields = 'items/id/videoId,nextPageToken',
                                    videoCategoryId = categoryId
                                   ).execute()
        upserts = [ UpdateOne(
                {'id':x["id"]["videoId"]},
                {
                    '$setOnInsert':{"id":x["id"]["videoId"]},
                    '$addToSet': {"regionCode": str(region)}
                }, upsert=True) for x in res["items"]]

        collectionMongo.bulk_write(upserts)

        next_page_token = res.get('nextPageToken')



def main():

    prepare_logging()
    logging.basicConfig(filename='./logs/log_API.log', level=logging.INFO)
    logging.info("Reading config files.")
    # Loading mongo database collection

    with open("configMongo.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)

    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]

    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]
    # We cannot set the collection name from API
    collectionName = "API"
    collectionMongo = databaseMongo[collectionName]
    # Shard the collection based on video id
    collectionMongo.create_index([ ('id', "hashed")])
    clientMongo.admin.command("shardCollection", str(MONGO_DATABASE)+"."+str(collectionName), key={"id":"hashed"})


    with open("configAPI.yml", "r") as file:
        cfgAPI = yaml.safe_load(file)
    # The largest number of video returned by search engine. The range is [0,50].
    MAXIMUM = cfgAPI["videosPerPage"]
    # If nothing is written, get the maximum value allowed
    if MAXIMUM == None:
        MAXIMUM = 50
    PAGES = cfgAPI["numPages"]
    # I want to pick up data in this range [1-1-2019, 31-12-2019]. It cuold be modified!
    first_date = cfgAPI["date"]["first_date"].strftime('%Y-%m-%dT%H:%M:%SZ')
    second_date = cfgAPI["date"]["second_date"].strftime('%Y-%m-%dT%H:%M:%SZ')
    region_list =  cfgAPI["regionList"]
    categoryId_list = cfgAPI["categoryIds"]
    key = cfgAPI["key"]

    logging.info("Loading API.")
    youtube = build('youtube', 'v3', developerKey = key)
    logging.info("Loaded API.")

    for region in region_list:
        for i, categoryId in enumerate(categoryId_list):
            logging.info("Progress region "+str(region)+": "+str(int(i/len(categoryId_list)*10000)/100.)+str("%"))
            try:
                set_video(key, region, PAGES, MAXIMUM, first_date, second_date, collectionMongo, youtube, categoryId)
            except:
                logging.info("Process stopped early")
                break

    logging.info("Data saved successfully")




if __name__ == "__main__":
    main()
