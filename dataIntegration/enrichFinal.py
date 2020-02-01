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

counter = 0 # <---- IMPORTANT FOR MEAN POLARITY

def suffix_number(numbers, specification_letter):
    if(specification_letter == ""):
        return numbers
    if(specification_letter == "K"):
        return numbers * 1E3
    elif(specification_letter == "M"):
        return numbers * 1E6
    elif(specification_letter == "B"):
        return numbers * 1E9

def capital_letters(titolo):
    from_numbers_to_letters = {
        '0': ['A',0] ,
        '1': ['B',0],
        '2': ['C',0],
        '3': ['D',0],
        '4': ['E',0],
        '5': ['F',0],
        '6': ['G',0],
        '7': ['H',0],
        '8': ['I',0],
        '9': ['J',0],
        '10': ['K',0],
        '11': ['L',0],
        '12': ['M',0],
        '13': ['N',0],
        '14': ['O',0],
        '15': ['P',0],
        '16': ['Q',0],
        '17': ['R',0],
        '18': ['S',0],
        '19': ['T',0],
        '20': ['U',0],
        '21': ['V',0],
        '22': ['W',0],
        '23': ['X',0],
        '24': ['Y',0],
        '25': ['Z',0]
    }
    if(titolo != None):
    #     Trovo esattamente queli lettere sono in maiuscolo.
        for number in range (0, len(from_numbers_to_letters)):
            from_numbers_to_letters[str(number)][1]+=len(re.findall(from_numbers_to_letters[str(number)][0], str(titolo)))
    #     Concludo dividendo il numero di lettere maiscole con quelle totali della stringa.
        counter = 0
        for number in range (0, len(from_numbers_to_letters)):
            counter +=  from_numbers_to_letters[str(number)][1]
        return counter
    else:
        return None

def title_analizer(titolo):
    diz = {} # farò embedding nel documento oririnale.
    if titolo != None:
        # Stringa del titolo.
        diz['text'] = str(titolo)

        # Lunghezza della stringa del titolo.
        diz['length'] = len(str(titolo))

        # La prima lettera è maiuscola?
        diz['first_letter_is_capital'] = str(titolo)[0].isupper()

        # Frazione di lettere maiuscole.
        diz['perc_capital_letter'] = capital_letters(titolo)/ len(str(titolo))

        # Contiene lo slash?
        diz['slash'] = len(re.findall('/', titolo))

        # Contiene pipe?
        diz['pipe'] = len(re.findall('\|', titolo))

        # Contiene -?
        diz['dash'] = len(re.findall('-', titolo))

        # Contiene !
        diz['exclamation_mark'] = len(re.findall('!', titolo))

        diz['asterisk'] = len(re.findall('\*', titolo))

        # Contiene il punto interrogativo?
        diz['question_mark'] = len(re.findall('\?', titolo)) # è un metacarattere che quindi vuole \.

        # Contiene &?
        diz['e_comm'] = len(re.findall('&', titolo))

        # Contiene l'hashtag?
        diz['hashtag'] = len(re.findall('#', titolo))

        # C'è stato un featuring?
        diz['featuring'] = bool(re.findall('(ft|feat|FEAT|featuring)', titolo))

        return diz
    else:
        diz['text'] = None

        # Lunghezza della stringa del titolo.
        diz['length'] = None

        # La prima lettera è maiuscola?
        diz['first_letter_is_capital'] = None

        # Frazione di lettere maiuscole.
        diz['perc_capital_letter'] = None

        # Contiene lo slash?
        diz['slash'] = None

        # Contiene pipe?
        diz['pipe'] = None

        # Contiene -?
        diz['dash'] = None

        # Contiene !
        diz['exclamation_mark'] = None

        diz['asterisk'] = None

        # Contiene il punto interrogativo?
        diz['question_mark'] = None

        # Contiene &?
        diz['e_comm'] = None

        # Contiene l'hashtag?
        diz['hashtag'] = None

        # C'è stato un featuring?
        diz['featuring'] = None


def dominant_color(img):
    if(img != None):
        NUM_CLUSTERS = 1
        img = img.resize((150,150))
        arr = np.asarray(img)
        shape = arr.shape
        arr = arr.reshape(scipy.product(shape[:2]), shape[2]).astype(float)
        codes, dist = scipy.cluster.vq.kmeans(arr, NUM_CLUSTERS)
        vecs, dist = scipy.cluster.vq.vq(arr, codes)
        counts, bins = scipy.histogram(vecs, len(codes))    # count occurrences
        index_max = scipy.argmax(counts)                    # find most frequent
        peak = codes[index_max]
        return list(peak)
    else:
        return None

def clean_replies(comment):
    text_string = comment["replies count"]

    try:
        # If digit number
         numbers =  int(re.sub(r"\D","",text_string))
    except:
        if(text_string == "View reply"):
            return 1
        else:
            return None

    specification_letter = re.sub(r"[^A-R]","",text_string)
    return suffix_number(numbers, specification_letter)


def hashtags_analizer(description):
    if description != None:
        return {
            'text': description,
            'hashtags': list(dict.fromkeys(re.findall('#[a-zA-Z-0-9]*', description)))
        }
    else:
        return None

def polarity_analizer(diz):
    polarity = TextBlob(diz['text']).sentiment.polarity
    global counter
    counter += polarity
    return polarity

def subjectivity_analizer(diz):
    return TextBlob(diz['text']).sentiment.subjectivity

def mean_polarity_analizer(diz):
    global counter
    if len(diz["comments"]) != 0:
        mean_polarity = counter/len(diz["comments"])
    else:
        mean_polarity = None
    counter = 0
    return mean_polarity

def frac_likes_analizer(diz):
    if diz['views'] != 'Unknown':
        if diz['likes'] != "Unknown":
            return float(diz['likes'])/int(diz['views'])
        else:
            return None
    else:
        return None

def frac_dislikes_analizer(diz):
    if diz['views'] != 'Unknown':
        if diz['dislikes'] != "Unknown":
            return float(diz['dislikes'])/int(diz['views'])
        else:
            return None
    else:
        return None

def domimantColor_analizer(diz):
        try:
            base64_string = diz["thumbnail_as_bytes"]
            base64_bytes = base64_string.encode("utf-8")
            bytes = base64.b64decode(base64_bytes)
            img = Image.open(io.BytesIO(bytes))
            return dominant_color(img)
        except:
            return None

def replies_count_analizer(diz):
    if(diz["replies count"] == "Unknown"):
        return None
    else:
        return clean_replies(diz)


def main():

    try:
        LINE_START = sys.argv[1]
    except:
        LINE_START = 0

    comm = MPI.COMM_WORLD

    ID = comm.Get_rank() + int(LINE_START)

    logging.basicConfig(filename='./logs/log_enrichFinal'+str(ID)+'.log', level=logging.INFO)
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
    logging.info("Mongo collections loaded.")

    BATCH_SIZE = round(scraperCollection.count_documents({})/comm.Get_size() + 0.5)
    cursorScraper = scraperCollection.find(no_cursor_timeout=True).skip(BATCH_SIZE*ID).limit(BATCH_SIZE)
    logging.info("Preparing to update.")
    upserts = [ UpdateOne(
        {'id':scraperDoc["id"], "ref_date": scraperDoc["ref_date"]},
        {
            '$set': {
                    "title": title_analizer(scraperDoc["title"]),
                    "description": hashtags_analizer(scraperDoc["description"]),
                    "frac_likes": frac_likes_analizer(scraperDoc),
                    "frac_dislikes": frac_dislikes_analizer(scraperDoc),
                    "dominantColor": domimantColor_analizer(scraperDoc)
                    }
        }) for scraperDoc in cursorScraper]
    upserts2 = [ UpdateOne(
        {'id':scraperDoc["id"], "ref_date": scraperDoc["ref_date"], "comments": {'$exists': True}},
        {
            '$set': {
                        "comments": [{
                                        "text": comment["text"],
                                        "likes": comment["likes"],
                                        "replies count": replies_count_analizer(comment),
                                        "user": comment["user"],
                                        "polarity": polarity_analizer(comment),
                                        "subjectivity": subjectivity_analizer(comment)} for comment in scraperDoc["comments"]],
                        "mean_polarity_comments": mean_polarity_analizer(scraperDoc),
                    }
        })for scraperDoc in cursorScraper]
    upserts.extend(upserts2)
    logging.info("Updating documents.")
    scraperCollection.bulk_write(upserts, ordered=False)
    cursorScraper.close()
    logging.info("Data saved succesfully to Mongo and cursor closed.")


if __name__ == "__main__":
    main()
