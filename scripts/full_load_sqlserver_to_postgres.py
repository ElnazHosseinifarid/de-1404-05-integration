import os
import pyodbc
import psycopg2
from psycopg2 import sql


SQL_SERVER = "localhost,21433"
SQL_DATABASE = "Northwind_BI_1404_05_DW"
SQL_USER = "sa"

PG_HOST = "localhost"
PG_PORT = 25432
PG_DATABASE = "sales_products"
PG_USER = "admin"

TABLES = [
    "DimCustomer",
    "DimDate",
    "DimEmployees",
    "DimGeography",
    "DimProducts",
    "DimShippers",
    "DimSuppliers",
    "DimTerritories",
    "FactEmployeeTerritories",
    "FactOrders",
]


def get_sqlserver_connection():
    password = os.environ["MSSQL_SA_PASSWORD"]

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)


def get_postgres_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=os.environ["PG_PASSWORD"],
    )


def normalize_value(value):
    return value


def load_table(sql_conn, pg_conn, table_name):
    sql_cur = sql_conn.cursor()
    pg_cur = pg_conn.cursor()

    source_table = f"dbo.{table_name}"
    target_table = table_name.lower()

    print(f"\n--- Loading {source_table} -> staging.{target_table} ---")

    # Read source columns
    sql_cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        table_name,
    )

    columns = [row[0] for row in sql_cur.fetchall()]

    if not columns:
        raise RuntimeError(f"No columns found for {source_table}")

    # Read source data
    column_list_sql = ", ".join(
        f"[{column.replace(']', ']]')}]"
        for column in columns
    )

    sql_cur.execute(
        f"SELECT {column_list_sql} FROM dbo.[{table_name}]"
    )

    rows = sql_cur.fetchall()

    source_count = len(rows)

    print(f"Source rows: {source_count}")
    print(f"Columns: {len(columns)}")

    # Empty target
    pg_cur.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier("staging"),
            sql.Identifier(target_table),
        )
    )

    # Prepare INSERT
    column_identifiers = sql.SQL(", ").join(
        sql.Identifier(column.lower())
        for column in columns
    )

    placeholders = sql.SQL(", ").join(
        sql.Placeholder() for _ in columns
    )

    insert_query = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES ({})"
    ).format(
        sql.Identifier("staging"),
        sql.Identifier(target_table),
        column_identifiers,
        placeholders,
    )

    # Insert data
    if rows:
        pg_cur.executemany(insert_query, rows)

    pg_conn.commit()

    # Validate destination count
    pg_cur.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier("staging"),
            sql.Identifier(target_table),
        )
    )

    target_count = pg_cur.fetchone()[0]

    print(f"Target rows: {target_count}")

    if source_count != target_count:
        raise RuntimeError(
            f"COUNT MISMATCH for {table_name}: "
            f"source={source_count}, target={target_count}"
        )

    print(f"STATUS: OK")


def main():
    print("=" * 70)
    print("SQL SERVER -> POSTGRESQL STAGING | FULL LOAD")
    print("=" * 70)

    sql_conn = None
    pg_conn = None

    try:
        sql_conn = get_sqlserver_connection()
        print("SQL Server connection: OK")

        pg_conn = get_postgres_connection()
        print("PostgreSQL connection: OK")

        for table in TABLES:
            load_table(sql_conn, pg_conn, table)

        print("\n" + "=" * 70)
        print("FULL LOAD COMPLETED SUCCESSFULLY")
        print("=" * 70)

    except Exception as exc:
        if pg_conn:
            pg_conn.rollback()

        print("\n" + "=" * 70)
        print("FULL LOAD FAILED")
        print("=" * 70)
        print(type(exc).__name__ + ":", exc)
        raise

    finally:
        if sql_conn:
            sql_conn.close()

        if pg_conn:
            pg_conn.close()


if __name__ == "__main__":
    main()
