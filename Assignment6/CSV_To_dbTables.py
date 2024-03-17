import pandas as pd
import pyodbc
import urllib
from sqlalchemy import create_engine

# Modify the connection strings based on your environment
# pyodbc connection string
conn_string_pyodbc = "Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=brocbenDW;Trusted_Connection=yes;"

# Construct the SQLAlchemy connection string
conn_string_sqlalchemy = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_string_pyodbc)

print("Finished")

try:
    # Test the connection using pyodbc
    conn_pyodbc = pyodbc.connect(conn_string_pyodbc)
    conn_pyodbc.close()
    print("Connection with pyodbc established successfully.")

    # Test the connection using sqlalchemy
    engine = create_engine(conn_string_sqlalchemy)
    with engine.connect():
        print("Connection with sqlalchemy established successfully.")

    # If both connections are successful, proceed with executing stored procedures and loading data from CSV files
    # Establish connection using pyodbc for executing stored procedures
    conn_pyodbc = pyodbc.connect(conn_string_pyodbc)
    cursor_pyodbc = conn_pyodbc.cursor()

    # Execute the stored procedures using pyodbc connection
    cursor_pyodbc.execute("dbo.DROP_TABLES")
    cursor_pyodbc.execute("dbo.CREATE_TABLES")
    cursor_pyodbc.commit()
    cursor_pyodbc.close()
    conn_pyodbc.close()

    # Create a sqlalchemy engine for loading data from CSV files
    engine = create_engine(conn_string_sqlalchemy)


    def csv_to_tables(fileName, tableName, conStr):
        df = pd.read_csv(fileName)
        # Count duplicates before removing
        duplicate_count = df.duplicated().sum()
        print(f"Found {duplicate_count} duplicates in {fileName}")

        # Remove duplicates from DataFrame
        df.drop_duplicates(inplace=True)
        df.to_sql(name=tableName, con=conStr, if_exists='append', index=False)

    # Load the tables from CSV files using sqlalchemy
    csv_to_tables('DimProducts.csv', 'DimProducts', engine)
    csv_to_tables('DimCustomers.csv', 'DimCustomers', engine)
    csv_to_tables('DimDate.csv', 'DimDate', engine)
    csv_to_tables('FactSales.csv', 'FactSales', engine)

    # Close the sqlalchemy engine
    engine.dispose()

    # Print data from each table in the database
    with engine.connect() as con:
        print("\nData from DimProducts table:")
        dim_products_data = pd.read_sql("SELECT TOP 10 * FROM DimProducts", con)
        print(dim_products_data)

        print("\nData from DimCustomers table:")
        dim_customers_data = pd.read_sql("SELECT TOP 10 * FROM DimCustomers", con)
        print(dim_customers_data)

        print("\nData from DimDate table:")
        dim_date_data = pd.read_sql("SELECT TOP 10 * FROM DimDate", con)
        print(dim_date_data)

        print("\nData from FactSales table:")
        fact_sales_data = pd.read_sql("SELECT TOP 10 * FROM FactSales", con)
        print(fact_sales_data)

except Exception as e:
    print("An error occurred while establishing the connection:", str(e))
