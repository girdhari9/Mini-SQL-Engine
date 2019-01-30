import sys
import csv
import operator 
import sqlparse
from collections import Counter
from collections import OrderedDict
import re
import os
from pprint import pprint
import copy 

metaDataDictionary = {}
columnsCountInTable = {}
aggregateFunction = ['max','min','avg','sum','distinct']
fromIndex = []

def queryExecution(parsedQuery, tokens):
    ####################### Handling Simple Select Query ###############################
    if (len(tokens) == 4):
        tablesName = getTablesName(tokens[3])
        if not checkTableExistance(tablesName):
            print("ERROR : Table does not exists!")
            return

        function = re.sub(r"[\(\)]",' ',tokens[1]).split()
        columnsName = []
        if (function[0] == '*'):
            for tab in tablesName:
                for columnName in metaDataDictionary[tab]:
                    columnsName.append(columnName)
        else:
            columnsName = getColName(tokens[1])
        
        if not function[0].lower() in aggregateFunction:
            fileData, colNames = multipleTableQuery(tokens, tablesName, columnsName, False)
            printHeader(columnsName, tablesName)
            printData(fileData)

        ####################### Preparing data for performing aggregate operations ###############################
        else:
            columnName = function[1]
            if '.' in columnName:
                colTab = columnName.split('.')
                columnName = colTab[1]
                tablesName = [colTab[0]]

            if not checkColumnAmb(tablesName, metaDataDictionary, columnName):
                sys.exit("ERROR : Ambiguous column name: " + columnName)

            fileData, colNames = multipleTableQuery(tokens, tablesName, columnName, False)
            if columnName not in colNames:
                sys.exit("ERROR : Invalid Attributes!")

            columnsName = [columnName]
            printHeader(columnsName, tablesName)

            colList = []
            if check == 0:
                for data in fileData:
                    colList.append(int(data[metaDataDictionary[tablesName[0]].index(columnName)]))
            else:
                for data in fileData:
                    colList.append(data[0])
            aggregateOperation(function[0].lower(), colList)

    ####################### Handling where statements ###############################
    else: 
        tablesName = getTablesName(tokens[fromIndex[0] + 1])
        if not checkTableExistance(tablesName):
            print("ERROR : Table does not exists!")
            return

        columnsName = []

        distinct = False
        if tokens[1] == 'distinct':
            distinct = True

        if tokens[fromIndex[0] - 1] == "*":
            for tableName in tablesName:
                for columnName in metaDataDictionary[tableName]:
                    columnsName.append(columnName)
        else:
            columnsName = getColName(tokens[fromIndex[0] - 1])

        for columnName in columnsName:
            if not checkColumnAmb(tablesName, metaDataDictionary, columnName):
                sys.exit("ERROR : Ambiguous column name: " + columnName)

        conditionData, join = processWhere(tokens, columnsName, tablesName, distinct)
        fileData, colNames = multipleTableQuery(tokens, tablesName, columnsName, distinct)
        
        if join and tokens[fromIndex[0] - 1] == "*":
            printHeader(colNames, tablesName, conditionData)
        elif tokens[fromIndex[0] - 1] == "*":
            printHeader(colNames, tablesName)
        else:
            printHeader(columnsName, tablesName)
        printData(fileData)

####################### Column's Max Value ###############################
def getMax(colList):
    maxVal = -9999999999
    for val in colList:
        if int(maxVal) < int(val):
            maxVal= val
    return maxVal

####################### Column's Min Value ###############################
def getMin(colList):
    minVal = 9999999999
    for val in colList:
        if int(minVal) > int(val):
            minVal= val
    return minVal

####################### Column Sum ###############################
def getSum(colList):
    total = 0
    for val in colList:
        total += int(val)
    return total

####################### Perform Aggregate Operation ###############################
def aggregateOperation(operation, colList):
    if(operation == 'max'):
        print(getMax(colList)) #Error
    elif(operation == 'min'):
        print(getMin(colList))
    elif(operation == 'sum'):
        print(getSum(colList))
    elif(operation == 'avg'):
        print(getSum(colList)/len(colList))
    elif(operation == 'distinct'):
        colList = list(OrderedDict.fromkeys(colList))
        for col in range(len(colList)):
            print(colList[col])
    else:
        print("ERROR: ","Unknown function : ", '"' + function[0] + '"')

####################### Check Ambiguous Columns ###############################
def checkColumnAmb(tablesName, metaDataDictionary, columnName):
    if len(tablesName) > 1:
        count = 0
        for tab in tablesName:
            if columnName in metaDataDictionary[tab]:
                if count > 0:
                    return False
                else:
                    count += 1
        return True
    else:
        return True

####################### Check condition of data ###############################
def processWhere(tokens, columnsName, tablesName, distinct):
    conditionData = re.sub(r"[\,]",' ',tokens[fromIndex[0] + 2]).split() #Error: write code to handle space
    del conditionData[0]
    join = False
    if len(conditionData) == 3 and '.' in conditionData[0] and '.' in conditionData[2] and '=' == conditionData[1]:
        join = True
    return conditionData, join

####################### Prepare data by applying condition ###############################
def whereQueryProcess(fileData, conditionData, columnsName, tablesName, join):
    index = -1
    if join:
        colNames, index = joinColumnProcess(columnsName, conditionData)
    
    processedData = []
    for data in fileData:
        string = evaluate(conditionData, tablesName, data)
        if eval(string):
            if not index == -1:
                data.pop(index)
                processedData.append(data)
            else:
                processedData.append(data)
    return processedData

####################### Check Table Existance ###############################
def checkTableExistance(tablesName):
    for tab in tablesName:
        if not os.path.exists(tab + '.csv'):
            return False
    return True

####################### Remove a column data while condition on columns ###############################
def joinColumnProcess(columnsName, conditionData):
    col1 = conditionData[0].split('.')[1]
    col2 = conditionData[2].split('.')[1]
    col1Index = columnsName.index(col1)
    col2Index = columnsName.index(col2)
    if col1 == col2 or conditionData[1] == "=":
        if col1Index < col2Index:
            columnsName.pop(col1Index)
            return columnsName, col1Index
        else:
            columnsName.pop(col2Index)
            return columnsName, col2Index

####################### Evaluate Where Condition ###############################
def evaluate(conditionData, tableNames, data):
    string = ""
    op = ['<=', '>=', '<', '>', 'and', 'or']
    for i in conditionData:
        if '.' in i:
            temp = i.split('.')
            if temp[1] in metaDataDictionary[temp[0]] and temp[0] in tableNames:
                string += data[metaDataDictionary[temp[0]].index(temp[1]) + columnsCountInTable[temp[0]]]
            else:
                sys.exit("ERROR : " + i + ' not exist!')
        elif i == '=':
            string += i*2
        elif i.lower() not in op and i.isalpha() == True:
            for tName in tableNames:
                if i in metaDataDictionary[tName]:
                    string += data[metaDataDictionary[tName].index(i)]
        elif i.lower() == 'and' or i.lower() == 'or':
            string += ' ' + i.lower() + ' '
        else:
            string += i
    return string

####################### To get data from multiple Tables ###############################
def multipleTableQuery(tokens, tablesName, attributes, distinct, conditionData = [''], join = False):
    fileData = joinQuery(tablesName, metaDataDictionary)
    colNames = copy.deepcopy(fileData[0])
    del fileData[0]
    for col in attributes:
        if not col in colNames:
            print("ERROR: Invalid Query!", "Attributes does not exist!")
            return

    if distinct == True:
        fileData = getDistinctData(fileData)

    if len(tokens) > fromIndex[0] + 2:
        fileData = whereQueryProcess(fileData, conditionData, colNames, tablesName, join)

    colIndex = []
    for col in attributes:
        index = 0
        for i in colNames:
            if i == col:
                colIndex.append(index)
            index += 1

    for item in fileData:
        for col in range(len(item)-1,-1,-1):
            if col not in colIndex:
                del item[col]

    return fileData, colNames

####################### To Get Distinct Data ###############################
def getDistinctData(fileData):
    distinctData = []
    for row in fileData:
        if row not in distinctData:
            distinctData.append(row)   
    return distinctData

####################### Helper of Join Tables ###############################
def joinTwo(table1, table2):
    if len(table1) == 0:
        return table2
    main_table = list()
    main_table.append(table1[0] + table2[0])
    for i in range(1,len(table1)):
        for j in range(1,len(table2)):
            main_table.append(table1[i]+table2[j])
    return main_table

####################### Join Tables ###############################
def joinQuery(table_list, table_columns):
    main_table = list()
    for table_name in table_list:
        data = list(csv.reader(open(table_name + '.csv')))
        data.insert(0, table_columns[table_name])
        main_table = joinTwo(main_table,data)
    return main_table

####################### Print Output Data ###############################
def printData(fileData):
    for data in fileData:
        string = ""
        for col in data:
            if not string ==  "":
                string += ','
            string += col
        print(string)

####################### Print Header of output Data ###############################
def printHeader(columnNames,tableNames, conditionData = ['']):
    print("output:")
    string = []
    for tab in tableNames:
        for col in columnNames:
            if col in metaDataDictionary[tab]:
                if (tab + '.' + col) not in string:
                    if len(string):
                        string.append(',')
                    if (tab + '.' + col) != str(conditionData[0]):
                        string.append(tab + '.' + col)
    for item in string:
        print(item, end="")
    print()

####################### Read Tables Data ###############################
def readFile(tName,fileData):
    csvfile = open(tName)
    dialect = csv.Sniffer().sniff(csvfile.read(1024))
    csvfile.seek(0)
    reader = csv.reader(csvfile, dialect)
    for row in reader:
        fileData.append(row)

####################### Separate Columns Name ###############################
def getColName(token):
    return re.sub(r"[\,]",' ',token).split()

####################### Separate Tables Name ###############################
def getTablesName(token):
    return re.sub(r"[\,]",' ',token).split()

####################### Read Meta Data ###############################
def readMetadata():
    f = open('./metadata.txt','r')
    check = 0
    for line in f:
        if line.strip() == "<begin_table>":
            check = 1
            continue
        if check == 1:
            tableName = line.strip()
            metaDataDictionary[tableName] = []
            # Keep track of total columns in previous tables
            columnsCountInTable[tableName] = 0
            for val in metaDataDictionary:
                columnsCountInTable[tableName] += len(metaDataDictionary[val]) 
            check = 0
            continue
        if not line.strip() == '<end_table>':
            metaDataDictionary[tableName].append(line.strip())

####################### Parse Query ###############################
def parseQuery(query):
    try:
        parsedQuery = sqlparse.parse(query)[0].tokens
        queryType = sqlparse.sql.Statement(parsedQuery).get_type()
        identifierList = []
        l = sqlparse.sql.IdentifierList(parsedQuery).get_identifiers()
        
        for i in l:
            identifierList.append(str(i))
        if len(identifierList) < 4:
            sys.exit("ERROR : Invalid Query!")

        if (str(queryType) == 'SELECT'):
            tokens = [item.lower() for item in identifierList]
            fromIndex.append(tokens.index('from'))

            if fromIndex == -1:
                sys.exit("ERROR : Syntex error!")
            queryExecution(parsedQuery, identifierList)
        else:
            print("Incorrect Query!\nOnly SELECT DML is supported!")
    except:
        print("Something went wrong!")

####################### Main Function ###############################
def main():
    readMetadata()
    while(1):
        print("msql>", end="")
        query = input()
        query = query.strip()
        if(query == "exit" or query == "quit"):
            break
        elif query.lower() == 'show database':
            print("Database Structure: ",metaDataDictionary)
        elif query[-1] is not ';':
            print("ERROR : Syntex Error!")
        else:
            parseQuery(query[:-1])

if __name__ == "__main__":
    main()