import pandas as pd
from sqlalchemy import create_engine
import pymysql
import base64
from mpi4py import MPI
import sys
import os
import logging
import bcolors
import shutil

try:
    LINE_START = sys.argv[1]
except:
    LINE_START = 0

# Start parallel session
comm = MPI.COMM_WORLD

ID = comm.Get_rank() + LINE_START




PASSWORD = base64.b64decode('Q2FjY2FGcml0dGE2OQ==').decode("utf-8")
db_connection_str = 'mysql+pymysql://giuseppe:'+str(PASSWORD)+'@tars1.bounceme.net:27050/videos'
db_connection = create_engine(db_connection_str)

file_list = ["CAvideos", "USvideos", "GBvideos"]

if comm.Get_rank() == 0:
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


logging.basicConfig(filename='./logs/log'+str(ID)+'.log', level=logging.INFO)


with open("datatasetKaggle/"+str(file_list[ID])+".csv", "r") as file:
    data = pd.read_csv(file)
logging.info("["+str(ID)+"] Data read.")
data.to_sql(con=db_connection, name=file_list[ID], if_exists='replace')
logging.info("["+str(ID)+"] Data succesfully saved.")
