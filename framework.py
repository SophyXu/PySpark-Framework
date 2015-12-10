__author__ = 'xxy'

import sys
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext


def fileOutput(sc, itemList, filePath, fileType) :

    sqlContext = SQLContext(sc)

    # JSON file
    if fileType == 'json':
        if itemList[0] == '*':
            df = sqlContext.jsonFile(filePath)

            # displays the content of the DataFrame to stdout
            df.show()

        else:
            df = sqlContext.load(filePath, "json")

            # select Error Detect!
            for item in itemList:
                df.select(item).show()

    # CSV file
    elif fileType == 'csv':
        # sc.textFile("file.csv")
        #     .map(lambda line: line.split(","))
        #     .filter(lambda line: len(line)<=1)
        #     .collect()
        pass

    # Hive
    elif fileType == 'hive':
        pass

    else:
        print("fileType Not Known.")

def parse(sc):
    # command: select * from * type *
    if sys.argv[1] == 'select':
        i = 2
        itemList = []
        while sys.argv[i] != 'from':
            itemList.append(sys.argv[i])
            i = i + 1
            if i >= len(sys.argv):
                print("Wrong Command!")
                exit(-1)

        filePath = sys.argv[i+1]
        if sys.argv[i+2] == 'type':
            fileType = sys.argv[i+3]
        else:
            print("Wrong Command!")
            exit(-1)            

        print(itemList)
        print(filePath)
        print(fileType)

        fileOutput(sc, itemList, filePath, fileType)


if __name__ == "__main__":

    # if len(sys.argv) != 2:
    #     print("Usage: sort <file>", file=sys.stderr)
    #     exit(-1)

    # spark context
    sc = SparkContext(appName="framework")

    parse(sc)

    sc.stop()