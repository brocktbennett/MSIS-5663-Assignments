import pandas as pd
import pyodbc
from tabulate import tabulate

# Modify the connection strings based on your environment
conn_string_pyodbc = "Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=brocbenDW;Trusted_Connection=yes;"

# Function to execute a query and write the result to a file
def execute_query_and_write_to_file(title, query, outFile, connection_string):
    try:
        # Attempt to connect to the database
        with pyodbc.connect(connection_string) as cnxn:
            print(f"Connection to database established successfully.")

            # Cursor to execute the query
            cursor = cnxn.cursor()
            cursor.execute(query)

            # Fetching the results from the executed query
            col_names = [i[0] for i in cursor.description]
            result = pd.DataFrame.from_records(cursor.fetchall(), columns=col_names)

            # Writing the result to the output file
            with open(outFile, 'a') as f:
                print("\n\n", title, file=f)
                print("\n\n", query, file=f)
                print("\n\n", tabulate(result, headers=col_names, tablefmt="grid", showindex="never"), file=f)

            print(f"Query results written to {outFile}")

    except pyodbc.Error as e:
        print(f"An error occurred while establishing the connection: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# List of queries and their titles
queries = [
    ("1. Total Quantity by Brand 'Surly'", """
    SELECT SUM(FactSales.quantity) AS Total_Quantity
    FROM FactSales
    JOIN DimProducts ON FactSales.product_id = DimProducts.Product_id
    WHERE DimProducts.brand_name = 'Surly';"""),

    ("2. Avg Discount for product category Mountain Bikes for customer living in state NY", """
    SELECT ROUND(AVG(FactSales.discount), 2) AS Average_Discount
    FROM FactSales
    JOIN DimProducts ON FactSales.product_id = DimProducts.Product_id
    JOIN DimCustomers ON FactSales.customer_id = DimCustomers.Customer_id
    WHERE DimProducts.category_name = 'Mountain Bikes' AND DimCustomers.state = 'NY';"""),

    ("3. Find the total undiscounted sales by product category", """
    SELECT COALESCE(Category_Name, 'All Categories') AS Category, FORMAT(SUM(List_Price * Quantity), 'C', 'en-US') AS Total_Undiscounted_Sales
    FROM FactSales FS
    JOIN DimProducts DP ON FS.product_id = DP.Product_id
    GROUP BY ROLLUP(Category_Name)
    ORDER BY Category DESC;"""),

    ("4. Drill Down Display the total undiscounted sales by product category for each year", """
    SELECT 
        COALESCE(dp.Category_name, 'All Categories') AS Category_name, 
        dp.model_year, 
        FORMAT(SUM(fs.List_Price * fs.Quantity), 'C', 'en-US') AS Total_Undiscounted_Sales
    FROM 
        FactSales fs
    JOIN 
        DimProducts dp ON fs.product_id = dp.product_id
    JOIN 
        DimDate dd ON fs.Order_Date_Key = dd.Date_Key
    GROUP BY 
        dp.Category_name, dp.model_year
    WITH ROLLUP
    ORDER BY 
        CASE 
            WHEN dp.Category_name IS NULL THEN ''
            ELSE dp.Category_name 
        END ASC,
        dp.model_year DESC;"""),

    ("5. Total Sales by Product Category for Every Combination", """
    SELECT 
        COALESCE(dp.Category_name, 'All Categories') AS Category_name, 
        COALESCE(dp.brand_name, 'All Brands') AS Brand_name, 
        FORMAT(SUM(fs.List_Price * fs.Quantity), 'C', 'en-US') AS Total_Undiscounted_Sales
    FROM 
        FactSales fs
    JOIN 
        DimProducts dp ON fs.product_id = dp.product_id
    GROUP BY CUBE(dp.Category_name, dp.brand_name)
    ORDER BY 
        CASE 
            WHEN dp.Category_name IS NULL THEN 'AAA' 
            ELSE dp.Category_name 
        END ASC,
        COALESCE(dp.brand_name, 'All Brands') DESC;"""),

    ("6. Top 10 Cities by Total Discounted Sales", """
    SELECT TOP 10
        dc.City,
        FORMAT(SUM((fs.List_Price - fs.Discount) * fs.Quantity), 'C', 'en-US') AS Total_Discounted_Sales
    FROM 
        FactSales fs
    JOIN 
        DimCustomers dc ON fs.customer_id = dc.Customer_id
    GROUP BY 
        dc.City
    ORDER BY 
        SUM((fs.List_Price - fs.Discount) * fs.Quantity) DESC;""")
]

out_file = 'out.txt'

# Execute each query and output results
for title, query in queries:
    execute_query_and_write_to_file(title, query, out_file, conn_string_pyodbc)

print("All queries have been executed and results written to", out_file)
