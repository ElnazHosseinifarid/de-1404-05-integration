import psycopg2
from clickhouse_driver import Client


PG_CONFIG = {
    "host": "127.0.0.1",
    "port": 25432,
    "database": "sales_products",
    "user": "admin",
    "password": "admin123",
}

CH_CONFIG = {
    "host": "127.0.0.1",
    "port": 29000,
    "user": "default",
    "password": "admin123",
    "database": "northwind_dw",
}


def main():
    print("FactOrders PostgreSQL -> ClickHouse started")

    pg_conn = psycopg2.connect(**PG_CONFIG)
    pg_cursor = pg_conn.cursor()
    print("PostgreSQL connection: OK")

    ch_client = Client(**CH_CONFIG)
    print("ClickHouse connection: OK")

    # ---------------------------------------------------------
    # 1. Read FactOrders from PostgreSQL Staging
    # ---------------------------------------------------------
    pg_cursor.execute("""
        SELECT
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
        FROM staging.factorders
        ORDER BY orderid, productkey
    """)

    rows = pg_cursor.fetchall()

    print("Rows extracted from PostgreSQL:", len(rows))

    # ---------------------------------------------------------
    # 2. Load source business-key mappings from PostgreSQL
    # ---------------------------------------------------------

    pg_cursor.execute("""
        SELECT
            customerkey,
            customeralternatekey
        FROM staging.dimcustomer
    """)

    customer_source_rows = pg_cursor.fetchall()

    customer_business_map = {
        source_key: alternate_key
        for source_key, alternate_key in customer_source_rows
    }

    pg_cursor.execute("""
        SELECT
            productkey,
            productalternatekey
        FROM staging.dimproducts
    """)

    product_source_rows = pg_cursor.fetchall()

    product_business_map = {
        source_key: alternate_key
        for source_key, alternate_key in product_source_rows
    }

    pg_cursor.execute("""
        SELECT
            employeekey,
            employeealternatekey
        FROM staging.dimemployees
    """)

    employee_source_rows = pg_cursor.fetchall()

    employee_business_map = {
        source_key: alternate_key
        for source_key, alternate_key in employee_source_rows
    }

    # ---------------------------------------------------------
    # 3. Load current SCD2 mappings from ClickHouse
    # ---------------------------------------------------------

    customer_current_rows = ch_client.execute("""
        SELECT
            customer_alternate_key,
            customer_key
        FROM northwind_dw.dim_customer
        WHERE is_current = 1
    """)

    customer_current_map = {
        alternate_key: dw_key
        for alternate_key, dw_key in customer_current_rows
    }

    product_current_rows = ch_client.execute("""
        SELECT
            product_alternate_key,
            product_key
        FROM northwind_dw.dim_products
        WHERE is_current = 1
    """)

    product_current_map = {
        alternate_key: dw_key
        for alternate_key, dw_key in product_current_rows
    }

    employee_current_rows = ch_client.execute("""
        SELECT
            employee_alternate_key,
            employee_key
        FROM northwind_dw.dim_employees
        WHERE is_current = 1
    """)

    employee_current_map = {
        alternate_key: dw_key
        for alternate_key, dw_key in employee_current_rows
    }

    # ---------------------------------------------------------
    # 4. Build final source-key -> current DW-key mappings
    # ---------------------------------------------------------

    customer_map = {}

    for source_key, alternate_key in customer_business_map.items():
        if alternate_key in customer_current_map:
            customer_map[source_key] = customer_current_map[alternate_key]

    product_map = {}

    for source_key, alternate_key in product_business_map.items():
        if alternate_key in product_current_map:
            product_map[source_key] = product_current_map[alternate_key]

    employee_map = {}

    for source_key, alternate_key in employee_business_map.items():
        if alternate_key in employee_current_map:
            employee_map[source_key] = employee_current_map[alternate_key]

    print("Customer mappings:", len(customer_map))
    print("Product mappings:", len(product_map))
    print("Employee mappings:", len(employee_map))

    # ---------------------------------------------------------
    # 5. Validate all FactOrders dimension mappings
    # ---------------------------------------------------------

    missing_customers = set()
    missing_products = set()
    missing_employees = set()

    for row in rows:
        source_product_key = row[2]
        source_customer_key = row[3]
        source_employee_key = row[4]

        if (
            source_customer_key is not None
            and source_customer_key not in customer_map
        ):
            missing_customers.add(source_customer_key)

        if source_product_key not in product_map:
            missing_products.add(source_product_key)

        if (
            source_employee_key is not None
            and source_employee_key not in employee_map
        ):
            missing_employees.add(source_employee_key)

    print("Missing customer keys:", sorted(missing_customers))
    print("Missing product keys:", sorted(missing_products))
    print("Missing employee keys:", sorted(missing_employees))

    if missing_customers or missing_products or missing_employees:
        raise RuntimeError(
            "FactOrders dimension mapping validation failed."
        )

    print("Dimension mapping validation: OK")

    # ---------------------------------------------------------
    # 6. Build final FactOrders rows
    # ---------------------------------------------------------

    insert_rows = []

    for row in rows:
        (
            order_id,
            geography_key,
            source_product_key,
            source_customer_key,
            source_employee_key,
            shipper_key,
            order_date_key,
            required_date_key,
            shipped_date_key,
            freight,
            ship_name,
            unit_price,
            quantity,
            discount,
            order_date,
            shipped_date,
            required_date,
        ) = row

        dw_product_key = product_map[source_product_key]

        dw_customer_key = (
            customer_map[source_customer_key]
            if source_customer_key is not None
            else None
        )

        dw_employee_key = (
            employee_map[source_employee_key]
            if source_employee_key is not None
            else None
        )

        insert_rows.append(
            (
                order_id,
                geography_key,
                dw_product_key,
                dw_customer_key,
                dw_employee_key,
                shipper_key,
                order_date_key,
                required_date_key,
                shipped_date_key,
                freight,
                ship_name,
                unit_price,
                quantity,
                discount,
                order_date,
                shipped_date,
                required_date,
            )
        )

    print("Fact rows prepared for ClickHouse:", len(insert_rows))

    # ---------------------------------------------------------
    # 7. Replace FactOrders in ClickHouse
    # ---------------------------------------------------------

    ch_client.execute("""
        TRUNCATE TABLE northwind_dw.fact_orders
    """)

    ch_client.execute("""
        INSERT INTO northwind_dw.fact_orders
        (
            order_id,
            geography_key,
            product_key,
            customer_key,
            employee_key,
            shipper_key,
            order_date_key,
            required_date_key,
            shipped_date_key,
            freight,
            ship_name,
            unit_price,
            quantity,
            discount,
            order_date,
            shipped_date,
            required_date
        )
        VALUES
    """, insert_rows)

    print("FactOrders inserted into ClickHouse:", len(insert_rows))

    # ---------------------------------------------------------
    # 8. Final validation
    # ---------------------------------------------------------

    ch_count = ch_client.execute("""
        SELECT count()
        FROM northwind_dw.fact_orders
    """)[0][0]

    pg_count = len(rows)

    print("PostgreSQL staging.factorders:", pg_count)
    print("ClickHouse fact_orders:", ch_count)

    if pg_count != ch_count:
        raise RuntimeError(
            f"FactOrders count mismatch: PostgreSQL={pg_count}, "
            f"ClickHouse={ch_count}"
        )

    print("FACT LOAD SUCCESS: Source and ClickHouse counts match.")

    pg_cursor.close()
    pg_conn.close()

    print("PostgreSQL connection closed")


if __name__ == "__main__":
    main()
