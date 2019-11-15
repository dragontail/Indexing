# Indexing

Coding Challenge #2

## Table of Contents
1. [Summary](#summary)
2. [Approach](#approach)
3. [Usage](#usage)

## Summary

The purpose of this coding challenge can be broken down into two steps:
1. Build a dictionary of words that appear in documents with unique IDs.
2. Use said dictionary to create an inverted index representing word IDs mapped to a list of all the document IDs that the words appear in.

Both the dictionary and the inverted index will be created as output files `/output/dictionary.csv` and `/output/index.json` respectively.

The structure of the directory is as follows:
```
Indexing
  |- src
    |- Indexing_Challenge.py
    |- Indexing_Challenge.ipynb
    |- Indexing_Challenge.dbc
  |- input
    |- 0
    |- 1
    |- ...
  |- output
    |- ...
```

## Approach

Spark is used to perform distributed computing on all of the files located in `/input`.
1. Build the dictionary
  - Create an RDD based on the text files for all of the input documents
  - Apply custom map function to the RDD that will place all of the words in a python dictionary
  - Since the computation is distributed, distinct() is applied to the RDD to remove duplicates
  - The RDD is then saved as a csv file to `/output/dictionary.csv`
2. Build the inverted index
  - Read the dictionary built in step 1 to a python dictionary
  - Create an empty RDD that will hold all of the results
  - Loop through each document
    - Obtain the filepath and the file number that represents the ID of the document
    - Create an RDD for the current document from the text file
    - Apply a custom map function to the RDD in order to create a dictionary with key-value pairs `{ wordID : [documentID] }`
    - Union the empty RDD with the current RDD to combine the two together
  - Reduce by key on the final result RDD in order to combine the lists of documentIDs for each word
  - The RDD is then sorted on both the keys and the values, and saved to a json file `/output/index.json`

## Usage

Development was done on a Databricks notebook. There are three files located in `/src` that can be imported to Databricks (the `.ipynb` file is recommended). From there, each block of code can be run in the UI. The data will have to be uploaded manually into a Databricks folder within the `FileStore/tables` system.
