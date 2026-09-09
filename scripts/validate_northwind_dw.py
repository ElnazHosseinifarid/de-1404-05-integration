from clickhouse_driver import Client


CH_CONFIG = {
    "host": "127.0.0.1",
    "port": 29000,
    "user": "default",
    "password": "admin123",
    "database": "northwind_dw",
}


EXPECTED_COUNTS = {
    "dim_customer": 92,
    "dim_date": 4017,
    "dim_employees": 10,
    "dim_geography": 150,
    "dim_products": 81,
    "dim_shippers": 3,
    "dim_suppliers": 33,
    "dim_territories": 54,
    "fact_orders": 2154,
    "fact_employee_territories": 49,
}


def main():
    print("Northwind DW Validation started")

    client = Client(**CH_CONFIG)
    client.execute("SELECT 1")

    print("ClickHouse connection: OK")

    failures = []

    print("\n--- Row Count Validation ---")

    for table_name, expected_count in EXPECTED_COUNTS.items():
        actual_count = client.execute(
            f"SELECT count() FROM northwind_dw.{table_name}"
        )[0][0]

        status = "OK" if actual_count == expected_count else "FAILED"

        print(
            f"{table_name}: "
            f"expected={expected_count}, "
            f"actual={actual_count} -> {status}"
        )

        if actual_count != expected_count:
            failures.append(
                f"{table_name} count mismatch"
            )

    print("\n--- SCD2 Validation ---")

    scd2_tables = {
        "dim_customer": "customer_alternate_key",
        "dim_employees": "employee_alternate_key",
        "dim_products": "product_alternate_key",
        "dim_suppliers": "supplier_alternate_key",
        "dim_territories": "territory_alternate_key",
    }

    for table_name, business_key in scd2_tables.items():

        invalid_current = client.execute(
            f"""
            SELECT count()
            FROM northwind_dw.{table_name}
            WHERE is_current = 1
              AND end_date IS NOT NULL
            """
        )[0][0]

        duplicate_current = client.execute(
            f"""
            SELECT count()
            FROM
            (
                SELECT {business_key}
                FROM northwind_dw.{table_name}
                WHERE is_current = 1
                GROUP BY {business_key}
                HAVING count() > 1
            )
            """
        )[0][0]

        print(
            f"{table_name}: "
            f"invalid_current={invalid_current}, "
            f"duplicate_current={duplicate_current}"
        )

        if invalid_current != 0:
            failures.append(
                f"{table_name} has invalid current rows"
            )

        if duplicate_current != 0:
            failures.append(
                f"{table_name} has duplicate current business keys"
            )

    print("\n--- Fact Dimension Integrity ---")

    orphan_checks = {
        "fact_orders.customer_key -> dim_customer": """
            SELECT count()
            FROM northwind_dw.fact_orders f
            LEFT JOIN northwind_dw.dim_customer d
                ON f.customer_key = d.customer_key
            WHERE f.customer_key IS NOT NULL
              AND d.customer_key IS NULL
        """,

        "fact_orders.product_key -> dim_products": """
            SELECT count()
            FROM northwind_dw.fact_orders f
            LEFT JOIN northwind_dw.dim_products d
                ON f.product_key = d.product_key
            WHERE d.product_key IS NULL
        """,

        "fact_orders.employee_key -> dim_employees": """
            SELECT count()
            FROM northwind_dw.fact_orders f
            LEFT JOIN northwind_dw.dim_employees d
                ON f.employee_key = d.employee_key
            WHERE f.employee_key IS NOT NULL
              AND d.employee_key IS NULL
        """,

        "fact_orders.geography_key -> dim_geography": """
            SELECT count()
            FROM northwind_dw.fact_orders f
            LEFT JOIN northwind_dw.dim_geography d
                ON f.geography_key = d.geography_key
            WHERE f.geography_key IS NOT NULL
              AND d.geography_key IS NULL
        """,

        "fact_orders.shipper_key -> dim_shippers": """
            SELECT count()
            FROM northwind_dw.fact_orders f
            LEFT JOIN northwind_dw.dim_shippers d
                ON f.shipper_key = d.shipper_key
            WHERE f.shipper_key IS NOT NULL
              AND d.shipper_key IS NULL
        """,

        "fact_employee_territories.employee_key -> dim_employees": """
            SELECT count()
            FROM northwind_dw.fact_employee_territories f
            LEFT JOIN northwind_dw.dim_employees d
                ON f.employee_key = d.employee_key
            WHERE d.employee_key IS NULL
        """,

        "fact_employee_territories.territory_key -> dim_territories": """
            SELECT count()
            FROM northwind_dw.fact_employee_territories f
            LEFT JOIN northwind_dw.dim_territories d
                ON f.territory_key = d.territory_key
            WHERE d.territory_key IS NULL
        """,
    }

    for check_name, query in orphan_checks.items():
        orphan_count = client.execute(query)[0][0]

        status = "OK" if orphan_count == 0 else "FAILED"

        print(
            f"{check_name}: "
            f"orphans={orphan_count} -> {status}"
        )

        if orphan_count != 0:
            failures.append(
                f"{check_name} has orphan keys"
            )

    print("\n--- Final Validation ---")

    if failures:
        print("VALIDATION FAILED")

        for failure in failures:
            print(f"- {failure}")

        raise RuntimeError(
            "Northwind DW validation failed."
        )

    print("VALIDATION SUCCESS")
    print("All row counts, SCD2 rules, and fact-dimension integrity checks passed.")


if __name__ == "__main__":
    main()
