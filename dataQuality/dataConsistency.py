import pandas as pd
import pymongo
import json
import re
import base64
from pymongo import UpdateOne
from pymongo import UpdateMany
from mpi4py import MPI
import sys
import logging
import bcolors
import os
import shutil
from datetime import datetime
import yaml
import numpy
import decimal

def elements_only(list):
    if "Unknown" in list:
        list.remove("Unknown")
    return list


def main():
    #prepare the logging files
    logging.basicConfig(filename='./logs/log_qualityChecks_kaggleCompleteness'+'.log', level=logging.INFO)
    logging.info("\n")
    logging.info("Log file created. Program started.")
    logging.info("Reading config files.")

    #prepare the mongo access from the config file
    with open("configMongoQuality.yml", "r") as file:
        cfgMongo = yaml.safe_load(file)
        logging.info("Config files succesfully read.")
        logging.info("Loading Mongo collections.")


    #select the database and the collection on mongoDB
    MONGO_HOST = cfgMongo["host"]
    MONGO_DATABASE = cfgMongo["database"]
    clientMongo = pymongo.MongoClient(MONGO_HOST)
    databaseMongo = clientMongo[MONGO_DATABASE]
    collectionName = "scrape"
    kaggleCollection = databaseMongo[collectionName]
    logging.info("Mongo collections loaded.")
    print("logged")

    #set up the value of iterations and sample sizes
    SAMPLE_SIZE = cfgMongo["sample_size"]
    FIELD_NAMES = cfgMongo["field_names"]

    #sample the collection selected
    sample_dict = kaggleCollection.aggregate(
      [{
        "$sample" : { "size" :  SAMPLE_SIZE }
      },{
        "$group" : {
                    "_id": "$id",
                    "ref_dates" : { "$addToSet" : "$ref_date" },
                    "titles" : { "$addToSet" : "$title" },
                    "channel_names" : { "$addToSet" : "$channel_name" },
                    "descriptions" : { "$addToSet" : "$description" },
                    "dates" : { "$addToSet" : "$date" },
                    }
      }]
    )
    print("queryed")
    # analizzo i risultati della query
    n_documents = 0
    title_changed = 0
    channel_changed = 0
    description_changed = 0
    date_error = 0
    for doc in sample_dict:
        if len(elements_only(doc["ref_dates"])) > 1 :
            n_documents += 1
            if len(elements_only(doc["titles"])) > 1:
                title_changed += 1
            if len(elements_only(doc["channel_names"])) > 1:
                channel_changed += 1
            if len(elements_only(doc["descriptions"])) > 1:
                description_changed += 1
            if len(elements_only(doc["dates"])) > 1:
                date_error += 1

    #print results
    print("calculated")
    result_diz = {
        "titles_changed" : {
            "mean" : title_changed/n_documents,
            "std" : numpy.sqrt((title_changed/n_documents)*(1 - title_changed/n_documents)/n_documents)
        },
        "channels_changed" : {
            "mean" : channel_changed/n_documents,
            "std" : numpy.sqrt((channel_changed/n_documents)*(1 - channel_changed/n_documents)/n_documents)
        },
        "descriptions_changed" : {
            "mean" : description_changed/n_documents,
            "std" : numpy.sqrt((description_changed/n_documents)*(1 - description_changed/n_documents)/n_documents)
        },
        "dates_changed" : {
            "mean" : date_error/n_documents,
            "std" : numpy.sqrt((date_error/n_documents)*(1 - date_error/n_documents)/n_documents)
        },
    }
    print("writing")
    with open("finaldataConsistency.txt", "w+") as outfile:
        json.dump(result_diz, outfile, indent = 4, sort_keys = True)
    print("Results saved in ./finaldataConsistency.txt")
    logging.info("Results saved in ./finaldataConsistency.txt")


if __name__ == "__main__":
  main()
