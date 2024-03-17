import pandas as pd
from tabulate import tabulate
import pyodbc
import os, sys
#connection_string ="Driver={ODBC Driver 17 for SQL Server};Server=SSBDN93JS3\\SQLEXPRESS;Database=MyStoreDW;Trusted_Connection=yes;"
connection_string ="Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=MyStoreDW;Trusted_Connection=yes;"
cnxn = pyodbc.connect(connection_string)
#
cursor = cnxn.cursor()
#
def queryResult(tItle,qUery, oUtFile):
    cursor.execute(my_query)
    col_names = [i[0] for i in cursor.description]
    result = pd.DataFrame.from_records(cursor, columns=col_names)
    with open(oUtFile, 'a') as f:
        print("\n\n", tItle, file=f)
        print("\n\n", qUery, file=f)
        print("\n\n", tabulate(result, headers = col_names, tablefmt="grid", showindex="always"), file=f)
#
title = "Undiscounted Sales By Year"
my_query = """select  D.[Year] AS Year, SUM(S.[list_price]*S.[quantity])AS Sales_By_Year
 from FactSales S, DimDate D
 where S.[Order_Date_Key] = D.[Date_Key]
 GROUP BY ROLLUP ( D.[Year]);"""
queryResult(title, my_query, 'out.txt')

#
