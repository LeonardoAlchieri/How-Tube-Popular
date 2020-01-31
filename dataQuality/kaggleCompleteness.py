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

def field_completeness(doc_list, doc_list_length, field_names):
    #field completeness is defined on a particular field of all the records
    mean_std_dict = { key : 0 for key in field_names}
    # iterate on all the documents
    for doc in doc_list:
        for key in mean_std_dict.keys():
            if doc[key] == None:
                mean_std_dict[key] += 1

    for (key, value) in mean_std_dict.items():
        p = value/doc_list_length
        mean_std_dict[key] = { 'mean' : "{:.4f}".format(p), 'std' : "{:.4f}".format(numpy.sqrt(p*(1-p)/doc_list_length))}
    return mean_std_dict;

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
    collectionName = "kaggleNation"
    kaggleCollection = databaseMongo[collectionName]
    logging.info("Mongo collections loaded.")

    #set up the value of iterations and sample sizes
    SAMPLE_SIZE = cfgMongo["sample_size"]
    FIELD_NAMES = cfgMongo["field_names"]

    #sample the collection selected
    sample_dict = kaggleCollection.aggregate(
        [{
            "$sample" : { "size" :  SAMPLE_SIZE }
        }]
    )
    result = field_completeness(sample_dict, SAMPLE_SIZE,FIELD_NAMES)
    with open("kaggleCompletenessResults.txt", "w+") as outfile:
        json.dump(result, outfile, indent = 4, sort_keys = True)
    print("Results saved in ./kaggleCompletenessResults.txt")
    logging.info("Results saved in ./kaggleCompletenessResults.txt")

if __name__ == "__main__":
    main()
