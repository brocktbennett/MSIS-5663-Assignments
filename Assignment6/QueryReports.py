import pandas as pd
import pyodbc
from tabulate import tabulate

# Base connection string without the Database parameter
base_conn_string_pyodbc = "Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Trusted_Connection=yes;"


def execute_query_and_write_to_file(title, query, outFile, database_name):
    # Modify the connection string to use the specified database
    connection_string = f"{base_conn_string_pyodbc}Database={database_name};"

    try:
        # Attempt to connect to the database
        with pyodbc.connect(connection_string) as cnxn:
            print(f"Connection to database {database_name} established successfully.")

            # Cursor to execute the query
            cursor = cnxn.cursor()
            cursor.execute(query)

            # Fetching the results
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


queries = [
    ("1. Total Quantity by Brand 'Surly'", """
    SELECT SUM(FactSales.quantity) AS Total_Quantity
    FROM FactSales
    JOIN DimProducts ON FactSales.product_id = DimProducts.Product_id
    WHERE DimProducts.brand_name = 'Surly';""", 'brocbenDW'),

    ("2. Avg Discount for product category Mountain Bikes for customer living in state NY", """
    SELECT ROUND(AVG(FactSales.discount), 2) AS Average_Discount
    FROM FactSales
    JOIN DimProducts ON FactSales.product_id = DimProducts.Product_id
    JOIN DimCustomers ON FactSales.customer_id = DimCustomers.Customer_id
    WHERE DimProducts.category_name = 'Mountain Bikes' AND DimCustomers.state = 'NY';""", 'brocbenDW'),

    ("3. Find the total undiscounted sales by product category", """
    SELECT COALESCE(Category_Name, 'All Categories') AS Category, FORMAT(SUM(List_Price * Quantity), 'C', 'en-US') AS Total_Undiscounted_Sales
    FROM FactSales FS
    JOIN DimProducts DP ON FS.product_id = DP.Product_id
    GROUP BY ROLLUP(Category_Name)
    ORDER BY Category DESC;""", 'brocbenDW'),

    ("4. Drill Down Display the total undiscounted sales by product category for each year", """
    SELECT COALESCE(dp.Category_name, 'All Categories') AS Category_name, dp.model_year, FORMAT(SUM(fs.List_Price * fs.Quantity), 'C', 'en-US') AS Total_Undiscounted_Sales
    FROM FactSales fs
    JOIN DimProducts dp ON fs.product_id = dp.product_id
    JOIN DimDate dd ON fs.Order_Date_Key = dd.Date_Key
    GROUP BY dp.Category_name, dp.model_year
    WITH ROLLUP
    ORDER BY CASE 
    WHEN dp.Category_name IS NULL THEN ''
    ELSE dp.Category_name 
    END ASC, dp.model_year DESC;""", 'brocbenDW'),

    ("5. Total Sales by Product Category for Every Combination", """
    SELECT COALESCE(dp.Category_name, 'All Categories') AS Category_name, COALESCE(dp.brand_name, 'All Brands') AS Brand_name, FORMAT(SUM(fs.List_Price * fs.Quantity), 'C', 'en-US') AS Total_Undiscounted_Sales
    FROM FactSales fs
    JOIN DimProducts dp ON fs.product_id = dp.product_id
    GROUP BY CUBE(dp.Category_name, dp.brand_name)
    ORDER BY CASE 
    WHEN dp.Category_name IS NULL THEN 'AAA' 
    ELSE dp.Category_name 
    END ASC, COALESCE(dp.brand_name, 'All Brands') DESC;""", 'brocbenDW'),

    ("6. Top 10 Cities by Total Discounted Sales", """
    SELECT TOP 10 dc.City, FORMAT(SUM((fs.list_price - (fs.list_price * fs.discount)) * fs.quantity), 'C', 'en-US') AS Total_Discounted_Sales 
    FROM FactSales fs 
    JOIN DimCustomers dc ON fs.customer_id = dc.Customer_id 
    GROUP BY dc.City 
    ORDER BY SUM((fs.list_price - (fs.list_price * fs.discount)) * fs.quantity) DESC;""", 'brocbenDW'),

    ("7. Comparing Dimensional Databases with Normalized Databases: Use the MyStoreDW (brocben)", """
    SELECT C.Category_Name as Category, FORMAT(SUM(oi.list_price * oi.quantity), 'C', 'en-US') AS Undiscounted_Sales
    FROM order_items as oi 
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    GROUP BY c.category_name
    UNION ALL 
    SELECT 'All Categories' AS Category, FORMAT(SUM(oi.list_price * oi.quantity), 'C', 'en-US') AS Undiscounted_Sales
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    ORDER BY Category DESC;""", 'brocben'),

    ("Code. Undiscounted Sales by Year", """
    select  D.[Year] AS Year, SUM(S.[list_price]*S.[quantity]) AS Sales_By_Year 
    from FactSales S, DimDate D 
    where S.[Order_Date_Key] = D.[Date_Key] 
    GROUP BY ROLLUP ( D.[Year]);""", 'brocbenDW'),
]

out_file = 'out.txt'
# Execute each query and output results
for title, query, database_name in queries:
    execute_query_and_write_to_file(title, query, out_file, database_name)
print("All finished")
print("All queries have been executed and results written to", out_file)
