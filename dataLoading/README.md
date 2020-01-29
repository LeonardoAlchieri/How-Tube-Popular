# Data Loading

We needed 2 scripts, one in order to load data from the Kaggle SQL database and one for data from the **Youtube API**. 

1. `kaggleMongo.py`: takes data from the Kaggle SQL database in *tars1* server and load onto the sharder database.
2. `APIMongo.py`: takes ids from **Youtube API** and loads them as a document on the sharded database.


