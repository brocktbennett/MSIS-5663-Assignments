import pandas as pd
import pyodbc
import urllib
from sqlalchemy import create_engine
#
conn_string = "Driver={ODBC Driver 17 for SQL Server};Server=SSBDN93JS3\\SQLEXPRESS;Database=MYStoreDW;Trusted_Connection=yes;"
#conn_string ="Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=MYStoreDW;Trusted_Connection=yes;"
conn = pyodbc.connect(conn_string)
cursor = conn.cursor()
#
# Execute the stored procedures using pyodbc connection string
cursor.execute("dbo.Drop_Tables")
cursor.execute("dbo.Create_Tables")
cursor.commit()
cursor.close()
conn.close()
#
#Connection string for sqlalchemy
connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=SSBDN93JS3\\SQLEXPRESS;Database=MYStoreDW;Trusted_Connection=yes;"
#connection_string ="Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=MYStoreDW;Trusted_Connection=yes;"
connection_string = urllib.parse.quote_plus(connection_string)
connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
#
# Create a sqlalchemy engine
engine = create_engine(connection_string)
#
def csv_to_tables(fileName,tableName,conStr):
    engine = create_engine(conStr)
    df = pd.read_csv(fileName)
    df.to_sql(name=tableName, con=engine, if_exists='append', index=False)
    engine.dispose()
#
# Load the tables from CSV files
csv_to_tables('DimProducts.csv','DimProducts',connection_string)
csv_to_tables('DimCustomers.csv','DimCustomers',connection_string)
csv_to_tables('DimDate.csv','DimDate',connection_string)
csv_to_tables('FactSales.csv','FactSales',connection_string)
#
# Close the engine
engine.dispose()
#


