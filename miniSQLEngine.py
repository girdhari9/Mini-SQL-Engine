import sys
import csv
import sqlparse
from collections import OrderedDict
import re
import os
import copy 

metaDataDictionary = {}
columnsCountInTable = {}
aggregateFunction = ['max','min','avg','sum','distinct']
fromIndex = []

def queryExecution(parsedQuery, tokens):
    ####################### Handling Simple Select Query ###############################
    if (len(tokens) == 4):
        tablesName = getTablesName(tokens[3])
        isExist, table = checkTableExistance(tablesName)
        if not isExist:
            print("Error: no such table: " + table)
            return

        function = re.sub(r"[\(\)]",' ',tokens[1]).split()
        columnsName = []
        if (function[0] == '*'):
            for tab in tablesName:
                for columnName in metaDataDictionary[tab]:
                    columnsName.append(columnName)
        else:
            columnsName = getColName(tokens[1])
            for columnName in columnsName:
                if not checkColumnAmb(tokens, tablesName, metaDataDictionary, columnName):
                    sys.exit("Error: Ambiguous column name: " + columnName)

        if not function[0].lower() in aggregateFunction:
            fileData, colNames = multipleTableQuery(tokens, tablesName, columnsName, False)
            printHeader(columnsName, tablesName)
            printData(fileData)

        ####################### Preparing data for performing aggregate operations ###############################
        else:
            columnName = function[1]
            check = 0
            if '.' in columnName:
                check= 1
                colTab = columnName.split('.')
                columnName = colTab[1]
                tablesName = [colTab[0]]

            if not checkColumnAmb(tokens, tablesName, metaDataDictionary, columnName):
                sys.exit("Error: Ambiguous column name: " + columnName)

            fileData, colNames = multipleTableQuery(tokens, tablesName, columnName, False)

            if columnName not in colNames:
                sys.exit("Error : no such column" + columnName)

            columnsName = [columnName]
            printHeader(columnsName, tablesName)

            colList = []
            for data in fileData:
                colList.append(data[0])
            aggregateOperation(function[0].lower(), colList)

    ####################### Handling where statements ###############################
    else:
        tablesName = getTablesName(tokens[fromIndex[0] + 1])
        isExist, table = checkTableExistance(tablesName)
        if not isExist:
            print("Error: no such table: " + table)
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
                if not checkColumnAmb(tokens, tablesName, metaDataDictionary, columnName):
                    sys.exit("Error: Ambiguous column name: " + columnName)

        conditionData, join = processWhere(tokens, columnsName, tablesName, distinct)
        fileData, colNames = multipleTableQuery(tokens, tablesName, columnsName, distinct, conditionData, join)

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
        print(getMax(colList))
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
        print("Error: ","Unknown function : ", '"' + function[0] + '"')

####################### Check Ambiguous Columns ###############################
def checkColumnAmb(tokens, tablesName, metaDataDictionary, columnName):
    colName = []
    if len(tokens) > fromIndex[0] + 2:
        conditionData = re.sub(r"[\,]",' ',tokens[fromIndex[0] + 2]).split() #Error: write code to handle space
        del conditionData[0]
        if '.' in conditionData[0] and '.' in conditionData[2] and '=' == conditionData[1]:
            colName.append(conditionData[2])
        if 'and' in conditionData:
            if '.' in conditionData[4] and '.' in conditionData[6] and '=' == conditionData[5]:
                colName.append(conditionData[6])

    if len(tablesName) > 1:
        count = 0
        for tab in tablesName:
            if columnName in metaDataDictionary[tab]:
                if columnName in colName:
                    continue
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
    if 'or' in conditionData:
        return conditionData, False
    if '.' in conditionData[0] and '.' in conditionData[2] and '=' == conditionData[1]:
        return conditionData, True
    if 'and' in conditionData:
        if '.' in conditionData[4] and '.' in conditionData[6] and '=' == conditionData[5]:
            return conditionData, True
    return conditionData, False
        
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
    return processedData

####################### Check Table Existance ###############################
def checkTableExistance(tablesName):
    for tab in tablesName:
        if not os.path.exists(tab + '.csv'):
            return False, tab
    return True, -1

####################### Remove a column data while condition on columns ###############################
def joinColumnProcess(columnsName, conditionData):
    col1, col2, cond = None, None, None

    if '.' in conditionData[0] and '.' in conditionData[2] and '=' == conditionData[1]:
        col1 = conditionData[0].split('.')[1]
        col2 = conditionData[2].split('.')[1]
        cond = conditionData[1]
    else:
        col1 = conditionData[4].split('.')[1]
        col2 = conditionData[6].split('.')[1]
        cond = conditionData[5]

    if col1 == col2 and cond == "=": 
        colIndex = rindex(columnsName, col1)
        columnsName.pop(colIndex) 
        return columnsName, colIndex

####################### To Find Index From Last ###############################
def rindex(mylist, myvalue):
    return len(mylist) - mylist[::-1].index(myvalue) - 1

####################### Evaluate Where Condition ###############################
def evaluate(conditionData, tableNames, data):
    string = ""
    op = ['<=', '>=', '<', '>', 'and', 'or']
    for i in conditionData:
        if '.' in i:
            temp = i.split('.')
            if temp[1] in metaDataDictionary[temp[0]] and temp[0] in tableNames:
                if '“' in data[metaDataDictionary[temp[0]].index(temp[1]) + columnsCountInTable[temp[0]]]:
                    string += data[metaDataDictionary[temp[0]].index(temp[1]) + columnsCountInTable[temp[0]]].split('“')[1][:-1]
                else:
                    string += data[metaDataDictionary[temp[0]].index(temp[1]) + columnsCountInTable[temp[0]]]
            else:
                sys.exit("Error: no such table: " + i)
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
        if '.' in col:
            colTab = col.split('.')
            if colTab[0] not in tablesName or colTab[1] not in metaDataDictionary[colTab[0]]:
                sys.exit("Error: no such column: " + col)

        elif not col in colNames:
            sys.exit("Error: no such column: " + col)

    if distinct == True:
        fileData = getDistinctData(fileData)

    if len(tokens) > fromIndex[0] + 2:
        fileData = whereQueryProcess(fileData, conditionData, colNames, tablesName, join)

    colIndex = []
    attributes = processColumnName(attributes)

    for col in attributes:
        index = 0
        for i in colNames:
            if i == col and index not in colIndex:
                colIndex.append(index)
                break
            index += 1
    if len(colIndex) < len(fileData[0]):
        newFileData = []
        for item in fileData:
            temp = []
            for col in colIndex:
                temp.append(item[col])
            newFileData.append(temp)
        return newFileData, colNames
    return fileData, colNames

####################### To get column name by saperating tablename ###############################
def processColumnName(columnsName):
    for col in range(len(columnsName)):
        if '.' in columnsName[col]:
            columnsName[col] = columnsName[col].split('.')[1]
    return columnsName

####################### To Get Distinct Data ###############################
def getDistinctData(fileData):
    distinctData = []
    for row in fileData:
        if row not in distinctData:
            distinctData.append(row)   
    return distinctData

####################### Helper of Join Tables ###############################
def joinTwo(table1, table2):
    if not len(table1):
        return table2
    joinTable = []
    data = table1[0] + table2[0]
    joinTable.append(data)
    for i in range(1,len(table1)):
        for j in range(1,len(table2)):
            data = table1[i]+table2[j]
            joinTable.append(data)
    return joinTable

####################### Join Tables ###############################
def joinQuery(tableList, table_columns):
    joinTable = []
    for tableName in tableList:
        data = list(readFile(tableName))
        data.insert(0, table_columns[tableName])
        joinTable = joinTwo(joinTable,data)
    return joinTable

####################### Remove Double Quotes From Data ###############################
def removeQuotes(fileData):
    for item in fileData:
        for index in range(len(item)):
            if '“' in item[index]:
                item[index] = item[index].split('“')[1][:-1]

####################### Print Output Data ###############################
def printData(fileData):
    removeQuotes(fileData)
    for data in fileData:
        string = ""
        for col in data:
            if not string ==  "":
                string += ','
            string += col
        print(string)

####################### Print Header of output Data ###############################
def printHeader(columnNames,tableNames, conditionData = ['']):
    columnNames =  processColumnName(columnNames)
    metadata = copy.deepcopy(metaDataDictionary)
    print("output:")
    string = []
    for col in columnNames:
        for tab in tableNames:
            if col in metadata[tab]:
                index = metadata[tab].index(col)
                del metadata[tab][index]
                if len(string):
                    string.append(',')
                string.append(tab + '.' + col)
                break
    for item in string:
        print(item, end="")
    print()

####################### Read Tables Data ###############################
def readFile(tName):
    return csv.reader(open(tName + '.csv'))

####################### Separate Columns Name ###############################
def getColName(token):
    return re.sub(r"[\,]",' ',token).split()

####################### Separate Tables Name ###############################
def getTablesName(token):
    return re.sub(r"[\,]",' ',token).split()


####################### Read Meta Data Function ###############################
def collectMetaData():
    if not os.path.exists('./metadata.txt'):
        sys.exit("Error: Metadata File Does Not Exist!")
    
    check = 0
    filePtr = open('./metadata.txt','r')
    for line in filePtr:
        if line.strip() == "<begin_table>":
            check = 1
            continue
        if check == 1:
            tableName = line.strip()
            # Keep track of total columns in previous tables
            columnsCountInTable[tableName] = 0
            metaDataDictionary[tableName] = []
            for val in metaDataDictionary:
                columnsCountInTable[tableName] += len(metaDataDictionary[val]) 
            check = not check
            continue
        if not line.strip() == "<end_table>":
            metaDataDictionary[tableName].append(line.strip())

####################### Parse Query Function ###############################
def parseQuery(query):
    try:
        parsedQuery = sqlparse.parse(query)[0].tokens
        queryType = sqlparse.sql.Statement(parsedQuery).get_type()
        identifierList = []
        l = sqlparse.sql.IdentifierList(parsedQuery).get_identifiers()
        
        for i in l:
            identifierList.append(str(i))

        if len(identifierList) < 4:
            sys.exit("Error: near \";\" : syntax error")

        if (str(queryType) == 'SELECT'):
            tokens = [item.lower() for item in identifierList]
            fromIndex.append(tokens.index('from'))

            if fromIndex == -1:
                sys.exit("Error: near \"SELECT\": syntax Error")
            queryExecution(parsedQuery, identifierList)
        else:
            print("Incorrect Query!\nOnly SELECT DML is supported!")
    except:
        print("Error: near \";\" : syntax error")

####################### Check Query ###############################
def checkValidQuery(query):
    query = query.strip()
    if(query == "exit" or query == "quit"):
        sys.exit()
    elif query.lower() == 'show database':
        print("Database: ",metaDataDictionary)
    elif query[-1] is not ';':
        parseQuery(query)
    else:
        parseQuery(query[:-1])

####################### Main Function ###############################
def main():
    collectMetaData()
    try:
        query = sys.argv[1]
        checkValidQuery(query)
    except:
        while(1):
            print("msql>", end="")
            query = input()
            checkValidQuery(query)

if __name__ == "__main__":
    main()