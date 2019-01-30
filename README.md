# Mini-Sql-Engine

### Description
Mini Sql Engine is one which runs a subset of SQL Queries using command line interface.

### SQL Query Syntax
1. select * from <tableName>
2. select aggregate(column) from <tableNames>
3. select <colnames> from <tableName> [ colnames = seperated only by ,]
4. select distinct(colName) from <tableNames>
5. select distinct(tableName.colName) from <tableNames>
5. select distinct <colnames> from <tableNames>
6. select <colNames> from <tableNames> where <conditions> [ seperated by space Ex: a = 1 and b = 2]
7. select * from <tableNames>
8. select <colNames> from <tableNames>
9. select <colnames> from <tableNames> where <join-condition>

### How to run
python code.py "SQL Query"

<tableName.colname> if [colnames = ambiguous]

