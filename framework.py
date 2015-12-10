__author__ = 'xxy'

import sys
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext

# Hive
def creatHive(sc, tableName, schema):
    sqlContext = HiveContext(sc)
    sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + schema + ")\""
    sqlContext.sql(sql)
    print("Creat hive table done.")

def creatHiveFromJson(sc, tableName, filePath):
    sqlContext = SQLContext(sc)
    jsonDataFrame = sqlContext.jsonFile(filePath)
    jsonDataFrame.registerAsTable(tableName)
    print("Creat hive table from json file done.")

def loadHive(sc, tableName, filePath):
    sqlContext = HiveContext(sc)
    sql = "LOAD DATA LOCAL INPATH " + "\'" + filePath + "\' INTO TABLE " + tableName
    sqlContext.sql(sql)
    print("Load hive data done.")

def selectHive(sc, tableName, items):
    sqlContext = HiveContext(sc)
    sql = "FROM " + tableName + " SELECT " + items
    results = sqlContext.sql(sql).collect()

# JSON
def selectJson(sc, itemList, filePath):
    sqlContext = SQLContext(sc)
    if itemList[0] == '*':
        df = sqlContext.jsonFile(filePath)

        # displays the content of the DataFrame to stdout
        df.show()

    else:
        df = sqlContext.load(filePath, "json")

        df.select(itemList).show()

# CSV
def selectCsv(sc, itemList, filePath):
    # sc.textFile("file.csv")
    #     .map(lambda line: line.split(","))
    #     .filter(lambda line: len(line)<=1)
    #     .collect()
    pass


def parseCommand(sc):

    # CREATE LOAD SELECT  
    var1 = sys.argv[2]
    var2 = sys.argv[4]
    fileType = sys.argv[6]        

    if sys.argv[1] == 'create':
        if fileType == 'hive':
            creatHive(sc, var1, var2)
        elif fileType == 'json':
            pass
        elif fileType == 'csv':
            pass
        else:
            print("Create file type error.")

    elif sys.argv[1] == 'load':
        if fileType == 'hive':
            loadHive(sc, var1, var2)
        elif fileType == 'json':
            pass
        elif fileType == 'csv':
            pass
        else:
            print("Load file type error.")

    elif sys.argv[1] == 'select':
        if fileType == 'hive':
            selectHive(sc, var1, var2)
        elif fileType == 'json':
            selectJson(sc, var1, var2)
        elif fileType == 'csv':
            selectCsv(sc, var1, var2)
        else:
            print("Select file type error.")


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: sort <file>", file=sys.stderr)
    #     exit(-1)

    # spark context
    sc = SparkContext(appName="framework")

    parseCommand(sc)

    sc.stop()