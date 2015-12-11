__author__ = 'xxy'

import sys
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext

# Hive
def creatHive(sc, tableName, schema):
    sqlContext = HiveContext(sc)
    # sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + schema + ")"
    # print(sql)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    # sqlContext.sql(sql)
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
        str = ''
        for index in printList:
            str += csvList[i][int(index)] + ' '
        print(str)

# column: , no space
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
        l = count * y - 1
        r = l + 1
        value = (x[l - 1] + x[r - 1]) * y
        return value
    else:
        l = int(count * y)
        value = x[l - 1]
        return value

def tableMedian(sc, column, tableName):
    pass
    # sqlCtx = SQLContext(sc)
    # df = sqlCtx.table(tableName)
    # df.sort(asc(column)).select(column).map(lambda x: percentile(x, 0.5)).collect()

def tablePercentile(sc, column, percentile,tableName):
    pass
    # sqlCtx = SQLContext(sc)
    # df = sqlCtx.table(tableName)
    # df.sort(asc(column)).select(column).map(lambda x: np.percentile(x, percentile)).collect()
   

def parse(sc): 
    VAR1 = sys.argv[2]
    VAR2 = sys.argv[4]
    TYPE = sys.argv[6]        

    # create NAME from PATH/SHEMA type TYPE
    if sys.argv[1] == 'create':
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