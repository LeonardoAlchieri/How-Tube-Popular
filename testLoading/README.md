# Loading tests

We try to load different parts of the dataset, and see how our architecture scales. We will do 25%, 50%, 75% and 100% for both raw collections and enriched collection.

In order to avoid doing API collection (which requires keys, difficult to make) and scraping (which takes a long time), we will upload from the local filesystem.
