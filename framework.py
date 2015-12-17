__author__ = 'xxy'

import sys
import json

from datetime import datetime, timedelta
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *

# Hive
def creatHive(sc, tableName, schema):
    sqlContext = HiveContext(sc)
    sql = "CREATE TABLE IsF NOT EXISTS " + tableName + " (" + schema + ")"
    print(sql)
    sqlContext.sql(sql)
    print("Creat hive table done.")

def creatHiveFromJson(sc, filePath, tableName):
    sqlContext = SQLContext(sc)
    jsonDataFrame = sqlContext.jsonFile(filePath)
    jsonDataFrame.registerAsTable(tableName)
    print("Creat hive table from json file done.")

def loadHive(sc, filePath, tableName):
    sqlContext = HiveContext(sc)
    sql = "LOAD DATA LOCAL INPATH " + "\'" + filePath + "\' INTO TABLE " + tableName
    sqlContext.sql(sql)
    print("Load hive data done.")

def selectHive(sc, columns, tableName):
    sqlContext = HiveContext(sc)
    sql = "FROM " + tableName + " SELECT " + columns
    results = sqlContext.sql(sql).collect()

# JSON
def selectJson(sc, columns, filePath):
    sqlContext = SQLContext(sc)
    if columns[0] == '*':
        df = sqlContext.jsonFile(filePath)

        # displays the content of the DataFrame to stdout
        df.show()

    else:
        df = sqlContext.load(filePath, "json")

        df.select(columns).show()

def loadJson(sc, filePath):
    sqlCtx = SQLContext(sc)
    df = sqlCtx.jsonFile(filePath)
    rdd = df.rdd
    rdd.map(lambda x : x[2]).collect()
    # rdd.sortBy(lambda x: x[2]).map(lambda x: x[2]).mean()

# CSV
def printCsvSchema(csvList):
    for i in range(len(csvList[0])):
        str = '|--' + csvList[0][i]
        print(str)

def printCsv(csvList, columns):
    printList = []

    for i in range(len(csvList[0])):
        for j in range(len(columns)):
            if columns[j] == csvList[0][i]:
                printList.append(i)
                columns[j] = ""

    # exception detect
    errorCol = ""
    flag = 0
    for column in columns:
        if column != "":
            flag = 1
            errorCol += column + " "
    if flag == 1:
        print("Error Column Input: " + errorCol)

    # print csv
    for i in range(len(csvList)):
        print("index " + i)
        str = ''
        for index in printList:
            str += csvList[i][int(index)] + ' '
        print(str)

def selectCsv(sc, columns, filePath):
    # sc.textFile("file.csv")
    #     .map(lambda line: line.split(","))
    #     .filter(lambda line: len(line)<=1)
    #     .collect()
    df = sc.textFile(filePath).map(lambda line: line.split(",")).collect()
    printCsvSchema(df)
    print(' ')
    if columns == '*':
        printCsv(df, df[0])
    else:
        printCsv(df, columns.split(","))
        
def loadCsv(sc, filePath, tableName):
    schema = StructType([StructField("TRX_DAT", StringType(), False), StructField("TRX_TIM", StringType(), False), StructField("CLT_COD", StringType(), False), StructField("TRX_AMT", IntegerType(), False), StructField("CTY_NAM", StringType(), False)])
    df = sc.textFile(filePath).map(lambda line: line.split(",")).collect()
    rdd = sc.parallelize(df)
    print(df)

# Hive table processing
def tableAvg(sc, tableName, Column):
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.groupBy().avg(Column).collect()
    return

def tableSum(sc, tableName, Column):
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.groupBy().sum(Column).collect()
    return

def tableMean(sc, tableName, Column):
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.groupBy().mean(Column).collect()
    return

def tableMax(sc, tableName, Column):
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.groupBy().max(Column).collect()
    return

def tableMin(sc, tableName, Column):
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.groupBy().min(Column).collect()
    return

# Hive table join
def tableJoin(sc, tb1, tb2, joinExp = None, joinType = None):
    sqlCtx = SQLContext(sc)
    df1 = sqlCtx.table(tb1)
    df2 = sqlCtx.table(tb2)
    df1.join(df2, joinExp, joinType)

def percentile(x, y):
    count = len(x)
    if count * y == int(count * y):
        l = int(count * y) - 1
        r = l + 1
        value = (x[l] + x[r]) * y
        return value
    else:
        l = int(count * y)
        value = x[l]
        return value

def tableMedian(sc, column, tableName):
    # pass
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.sort(asc(column)).select(column).map(lambda x: percentile(x, 0.5)).collect()

def tablePercentile(sc, column, percentile,tableName):
    # pass
    sqlCtx = SQLContext(sc)
    df = sqlCtx.table(tableName)
    df.sort(asc(column)).select(column).map(lambda x: np.percentile(x, percentile)).collect()

#########################
def timeComp(trxDays):
    now = datetime.now()
    yesterday = now - timedelta(days=int(trxDays))
    tmp1 = unicode(yesterday)
    tmp2 = tmp1[0:4] + tmp1[5:7] + tmp1[8:11]
    return int(tmp2)

def median(x, y):
    print(x, y)
    if count * 0.5 == int(count * 0.5):
        l = int(count * 0.5) - 1
        r = l + 1
        value = (x[l] + x[r]) * 0.5
        return value
    else:
        l = int(count * 0.5)
        value = x[l]
        return value

# rdd.sortBy(lambda x: x[2]).map(lambda x: x[2]).mean()

# done
def trxSingleAmountMean(rdd, startDate):
    trxSingleSum = rdd.filter(lambda x: int(x[3][0:4]+x[3][5:7]+x[3][8:11]) > startDate).map(lambda x : x[2]).sum()

# done
def trxDayAmountMean(rdd, startDate, days):
    trxDaySum = rdd.filter(lambda x: int(x[3][0:4]+x[3][5:7]+x[3][8:11]) > startDate).map(lambda x : (x[3],x[2])).reduceByKey(add).map(lambda x: x[1]).sum()
    trxDayMean = trxDaySum / days

def trxAmountMedian(rdd, startDate):
    rdd.filter(lambda x: int(x[3][0:4]+x[3][5:7]+x[3][8:11]) > startDate).map(lambda x : x[2]).reduce(lambda x,y: median(x,y))

# done
def trxDayTradeMean(rdd, startDate, days):
    trxDaySum = rdd.filter(lambda x: int(x[3][0:4]+x[3][5:7]+x[3][8:11]) > startDate).map(lambda x : (x[3],1)).reduceByKey(add).map(lambda x: x[1]).sum()
    trxDayMean = trxDaySum / days

def trxAmountSum(rdd, startDate):
    rdd.filter(lambda x: int(x[3][0:4]+x[3][5:7]+x[3][8:11]) > startDate).map(lambda x : x[2]).sum()

# done
def trxTradeCount(rdd, startDate):
    rdd.filter(lambda x: int(x[3][0:4]+x[3][5:7]+x[3][8:11]) > startDate).count()


def parse(sc): 
    VAR1 = sys.argv[2]
    VAR2 = sys.argv[4]
    TYPE = sys.argv[6]        

    # create NAME from PATH/SHEMA type TYPE
    if sys.argv[6] == 'where':
        trxAttri = sys.argv[2]
        trxStat = sys.argv[3]
        trxPath = sys.argv[5]
        trxDays = sys.argv[7]

        # json to rdd
        sqlCtx = SQLContext(sc)
        df = sqlCtx.jsonFile(trxPath)
        rdd = df.rdd
        
        startDate = timeComp(trxDays)

        if trxAttri == 'amount':
            if trxStat == 'mean':
                pass
            elif trxStat == 'median':
                # trxAmountMean(rdd, trxDays)
                trxAmountMedian(rdd, startDate)
                pass
            else:
                print("Transcation Attribute Wrong Type.")
        elif trxAttri == 'trade':
            if trxStat == 'mean':
                pass
            elif trxStat == 'median':
                pass
            else:
                print("Transcation Attribute Wrong Type.")
        else:
            print("Transcation Statistics Wrong Type.")                 

    elif sys.argv[1] == 'create':
        if TYPE == 'hive':
            creatHive(sc, VAR1, VAR2)
        elif TYPE == 'json':
            pass
        elif TYPE == 'csv':
            pass
        else:
            print("Create file type error.")

    # load FILE_PATH table TABLE_NAME type TYPE
    elif sys.argv[1] == 'load':
        if TYPE == 'hive':
            loadHive(sc, VAR1, VAR2)
        elif TYPE == 'json':
            pass
        elif TYPE == 'csv':
            pass
        else:
            print("Load file type error.")

    # select COLUMN(S) from TABLE_NAME type TYPE
    # select COLUMN(S) from FILE_PATH type TYPE
    elif sys.argv[1] == 'select':
        if TYPE == 'hive':
            selectHive(sc, VAR1, VAR2)
        elif TYPE == 'json':
            selectJson(sc, VAR1, VAR2)
        elif TYPE == 'csv':
            selectCsv(sc, VAR1, VAR2)
        else:
            print("Select file type error.")

    # table TABLE_NAME colunm COLUME_NAME(S) type TYPE
    elif sys.argv[1] == 'table':
        if TYPE == 'avg':
            tableAvg(sc, VAR1, VAR2)
        elif TYPE == 'sum':
            tableSum(sc, VAR1, VAR2)
        elif TYPE == 'mean':
            tableMean(sc, VAR1, VAR2)
        elif TYPE == 'max':
            tableMax(sc, VAR1, VAR2)
        elif TYPE == 'min':
            tableMin(sc, VAR1, VAR2)
        else:
            print("Table type error.")

    # join TABLE_NAME_1 TABLE_NAME_2 (exp JOIN_EXPRESSION) (type JOIN_TYPE)
    elif sys.argv[1] == 'join':
        if len(sys.argv) >= 5:
            if sys.argv[4] == 'exp':
                joinExp = sys.argv[5]
            elif sys.argv[4] == 'type':
                joinType = sys.argv[5]
        if len(sys.argv) >= 7:
                joinType = sys.argv[7]
        tableJoin(sc, sys.argv[2], sys.argv[3], joinExp = None, joinType = None)

    else:
        print("Command Error.")


if __name__ == "__main__": 
    # if len(sys.argv) != 2:
    #     print("Usage: sort <file>", file=sys.stderr)
    #     exit(-1)

    sc = SparkContext(appName="framework")

    parse(sc)

    sc.stop()