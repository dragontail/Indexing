# Databricks notebook source
# DBTITLE 1,Imports
import re
import io
from collections import defaultdict

# COMMAND ----------

# MAGIC %md ## Creating the dictionary

# COMMAND ----------

def buildDictionary(line: str):
    dictionary = {}
    
    words = " ".join((str(line)).split("\\r\\n")).split()
    for word in words:
        word = re.sub(r"^\W+|\W+$", "", word)
        if word == "":
          continue
          
        dictionary.setdefault(word, 0)

    return [k for k in dictionary.keys()]

# COMMAND ----------

# MAGIC %md Define the input directory and output files for dictionary/index

# COMMAND ----------

inputPath = "/FileStore/tables/"
outputFile = "/FileStore/output/dictionary.csv"
indexFile = "/FileStore/output/index.json"

# COMMAND ----------

# MAGIC %md Read in files from inputPath, create dictionary, and store result in outputFile

# COMMAND ----------

def toCSVLine(line):
  return " ".join(str(d) for d in line)
  
def createDictionary(inputPath, outputFile):
  directory = sc.textFile(inputPath).coalesce(1)
  dictionary = directory.flatMap(lambda file: buildDictionary(file)).distinct().filter(lambda line: line != "").zipWithIndex()
  dictionary.map(toCSVLine).saveAsTextFile(outputFile)

# COMMAND ----------

createDictionary(inputPath, outputFile)

# COMMAND ----------

sc.textFile(outputFile).collect()

# COMMAND ----------

def readDictionary(inputFile):
  dictionary = sc.textFile(inputFile).collect()
  dictionary = { word : id for word, id in [(item.split(" ")[0], item.split(" ")[1]) for item in dictionary]}
  return dictionary

# COMMAND ----------

dictionary = readDictionary(outputFile)

# COMMAND ----------

# MAGIC %md ## Creating the inverted index

# COMMAND ----------

def createIndex(inputPath, dictionary):
  # acquire all of the file paths for the given input path
  hadoop = sc._jvm.org.apache.hadoop
  fs = hadoop.fs.FileSystem
  conf = hadoop.conf.Configuration() 
  path = hadoop.fs.Path(inputPath)

  indices = sc.parallelize([])

  for f in fs.get(conf).listStatus(path):
    # distinguish between file path and name of file
    filePath = str(f.getPath())
    fileName = filePath.split('/')[-1]
    
    document = sc.textFile(filePath)
    index = document.flatMap(lambda line: buildIndex(line, dictionary, fileName))
    indices = indices.union(index)
    
  return indices.reduceByKey(lambda x, y: x+y).sortByKey().map(lambda x: (x[0], sorted(list(set(x[1])))))

# COMMAND ----------

# MAGIC %md Function to build an index of all words that appear in a given document
# MAGIC 
# MAGIC     document: the string containing all of the text of the document
# MAGIC     dictionary: a dictionary of all the words from documents with their ID
# MAGIC     number: the number corresponding to the file
# MAGIC     
# MAGIC     return: a dictionary with key value pairs of { wordID : documentID }

# COMMAND ----------

def buildIndex(document, dictionary, number):
  index = {}
  words = " ".join((str(document)).split("\\r\\n")).split()
  for word in words:
    word = re.sub(r"^\W+|\W+$", "", word)
    if word == "":
      continue
    
    if word in dictionary:
      index.setdefault(dictionary[word], number)
      
  return [(int(k), [int(v)]) for k, v in index.items()]

# COMMAND ----------

indices = createIndex(inputPath, dictionary)

# COMMAND ----------

df = indices.toDF(["word", "documents"])
df.write.save(indexFile, format = "json")

# COMMAND ----------

sc.textFile(indexFile).collect()

# COMMAND ----------


