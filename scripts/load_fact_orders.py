import os
import pyodbc
import psycopg2


# =====================================================
# 1. اتصال به SQL Server
# =====================================================

sql_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=127.0.0.1,21433;"
    "DATABASE=Northwind_BI_1404_05_DW;"
    "UID=sa;"
    "PWD=" + os.environ["MSSQL_SA_PASSWORD"] + ";"
    "TrustServerCertificate=yes;"
)

sql_cursor = sql_conn.cursor()

print("SQL Server connection: OK")


# =====================================================
# 2. اتصال به PostgreSQL
# =====================================================

pg_conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    database="Northwind",
    user="postgres",
    password=os.environ["PG_PASSWORD"]
)

pg_cursor = pg_conn.cursor()

print("PostgreSQL connection: OK")


# =====================================================
# 3. خواندن اطلاعات FactOrders از SQL Server
# =====================================================

sql_cursor.execute("""
    SELECT
        OrderID,
        GeographyKey,
        ProductKey,
        CustomerKey,
        EmployeeKey,
        ShipperKey,
        OrderdateKey,
        RequiredDateKey,
        ShippedDateKey,
        Freight,
        ShipName,
        UnitPrice,
        Quantity,
        Discount,
        OrderDate,
        ShippedDate,
        RequiredDate
    FROM dbo.FactOrders
""")

rows = sql_cursor.fetchall()

print("Rows extracted from SQL Server:", len(rows))


# =====================================================
# 4. پاک کردن داده قبلی Staging
# =====================================================

pg_cursor.execute("""
    TRUNCATE TABLE staging.fact_orders;
""")

print("Staging table truncated")


# =====================================================
# 5. درج اطلاعات در PostgreSQL
# =====================================================

insert_query = """
    INSERT INTO staging.fact_orders (
        orderid,
        geographykey,
        productkey,
        customerkey,
        employeekey,
        shipperkey,
        orderdatekey,
        requireddatekey,
        shippeddatekey,
        freight,
        shipname,
        unitprice,
        quantity,
        discount,
        orderdate,
        shippeddate,
        requireddate
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s
    )
"""

pg_cursor.executemany(insert_query, rows)

pg_conn.commit()

print("Rows loaded into PostgreSQL:", len(rows))


# =====================================================
# 6. کنترل تعداد رکوردها
# =====================================================

pg_cursor.execute("""
    SELECT COUNT(*)
    FROM staging.fact_orders;
""")

staging_count = pg_cursor.fetchone()[0]

print("PostgreSQL staging.fact_orders:", staging_count)


# =====================================================
# 7. کنترل نهایی
# =====================================================

if len(rows) == staging_count:
    print("ETL SUCCESS: Source and Staging counts match.")
else:
    print("ETL ERROR: Source and Staging counts do NOT match.")


# =====================================================
# 8. بستن اتصال‌ها
# =====================================================

sql_cursor.close()
sql_conn.close()

pg_cursor.close()
pg_conn.close()

print("Connections closed.")
