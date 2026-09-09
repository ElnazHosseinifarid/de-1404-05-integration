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
    print("FactEmployeeTerritories PostgreSQL -> ClickHouse started")

    pg_conn = psycopg2.connect(**PG_CONFIG)
    print("PostgreSQL connection: OK")

    ch_client = Client(**CH_CONFIG)
    ch_client.execute("SELECT 1")
    print("ClickHouse connection: OK")

    # ---------------------------------------------------------
    # 1. Extract FactEmployeeTerritories
    # ---------------------------------------------------------
    pg_cur = pg_conn.cursor()

    pg_cur.execute("""
        SELECT
            employeekey,
            territorykey
        FROM staging.factemployeeterritories
        ORDER BY employeekey, territorykey
    """)

    fact_rows = pg_cur.fetchall()

    print(f"Rows extracted from PostgreSQL: {len(fact_rows)}")

    # ---------------------------------------------------------
    # 2. Employee mapping
    # staging employee key -> employee alternate key
    # ---------------------------------------------------------
    pg_cur.execute("""
        SELECT
            employeekey,
            employeealternatekey
        FROM staging.dimemployees
    """)

    employee_source_map = {
        row[0]: row[1]
        for row in pg_cur.fetchall()
    }

    # ---------------------------------------------------------
    # 3. Territory mapping
    # staging territory key -> territory alternate key
    # ---------------------------------------------------------
    pg_cur.execute("""
        SELECT
            territorykey,
            territoryalternatekey
        FROM staging.dimterritories
    """)

    territory_source_map = {
        row[0]: row[1]
        for row in pg_cur.fetchall()
    }

    print(f"Employee source mappings: {len(employee_source_map)}")
    print(f"Territory source mappings: {len(territory_source_map)}")

    # ---------------------------------------------------------
    # 4. Current Employee DW mapping
    # employee alternate key -> current DW employee key
    # ---------------------------------------------------------
    employee_dw_rows = ch_client.execute("""
        SELECT
            employee_alternate_key,
            employee_key
        FROM northwind_dw.dim_employees
        WHERE is_current = 1
    """)

    employee_dw_map = {
        row[0]: row[1]
        for row in employee_dw_rows
    }

    # ---------------------------------------------------------
    # 5. Current Territory DW mapping
    # territory alternate key -> current DW territory key
    # ---------------------------------------------------------
    territory_dw_rows = ch_client.execute("""
        SELECT
            territory_alternate_key,
            territory_key
        FROM northwind_dw.dim_territories
        WHERE is_current = 1
    """)

    territory_dw_map = {
        row[0]: row[1]
        for row in territory_dw_rows
    }

    print(f"Employee DW mappings: {len(employee_dw_map)}")
    print(f"Territory DW mappings: {len(territory_dw_map)}")

    # ---------------------------------------------------------
    # 6. Build final fact rows
    # ---------------------------------------------------------
    final_rows = []
    missing_employee_keys = set()
    missing_territory_keys = set()

    for employee_source_key, territory_source_key in fact_rows:

        employee_alternate_key = employee_source_map.get(
            employee_source_key
        )

        territory_alternate_key = territory_source_map.get(
            territory_source_key
        )

        if employee_alternate_key not in employee_dw_map:
            missing_employee_keys.add(employee_source_key)
            continue

        if territory_alternate_key not in territory_dw_map:
            missing_territory_keys.add(territory_source_key)
            continue

        employee_dw_key = employee_dw_map[
            employee_alternate_key
        ]

        territory_dw_key = territory_dw_map[
            territory_alternate_key
        ]

        final_rows.append(
            (
                employee_dw_key,
                territory_dw_key,
            )
        )

    # ---------------------------------------------------------
    # 7. Mapping validation
    # ---------------------------------------------------------
    print(f"Missing employee source keys: {sorted(missing_employee_keys)}")
    print(f"Missing territory source keys: {sorted(missing_territory_keys)}")

    if missing_employee_keys or missing_territory_keys:
        raise RuntimeError(
            "Dimension mapping validation FAILED. "
            "No changes were made to ClickHouse."
        )

    if len(final_rows) != len(fact_rows):
        raise RuntimeError(
            "Prepared fact row count does not match source row count. "
            "No changes were made to ClickHouse."
        )

    print("Dimension mapping validation: OK")
    print(f"Fact rows prepared for ClickHouse: {len(final_rows)}")

    # ---------------------------------------------------------
    # 8. Backup existing FactEmployeeTerritories
    # ---------------------------------------------------------
    backup_count = ch_client.execute("""
        SELECT count()
        FROM northwind_dw.fact_employee_territories
    """)[0][0]

    print(f"Existing ClickHouse FactEmployeeTerritories rows: {backup_count}")

    ch_client.execute("""
        DROP TABLE IF EXISTS northwind_dw.fact_employee_territories_backup_before_final_load
    """)

    ch_client.execute("""
        CREATE TABLE northwind_dw.fact_employee_territories_backup_before_final_load
        AS northwind_dw.fact_employee_territories
    """)

    ch_client.execute("""
        INSERT INTO northwind_dw.fact_employee_territories_backup_before_final_load
        SELECT *
        FROM northwind_dw.fact_employee_territories
    """)

    print("Backup created: fact_employee_territories_backup_before_final_load")

    # ---------------------------------------------------------
    # 9. Reload target
    # ---------------------------------------------------------
    ch_client.execute("""
        TRUNCATE TABLE northwind_dw.fact_employee_territories
    """)

    ch_client.execute("""
        INSERT INTO northwind_dw.fact_employee_territories
        (
            employee_key,
            territory_key
        )
        VALUES
    """, final_rows)

    print(
        f"FactEmployeeTerritories inserted into ClickHouse: "
        f"{len(final_rows)}"
    )

    # ---------------------------------------------------------
    # 10. Final validation
    # ---------------------------------------------------------
    target_count = ch_client.execute("""
        SELECT count()
        FROM northwind_dw.fact_employee_territories
    """)[0][0]

    source_count = len(fact_rows)

    print(f"PostgreSQL staging.factemployeeterritories: {source_count}")
    print(f"ClickHouse fact_employee_territories: {target_count}")

    if source_count != target_count:
        raise RuntimeError(
            "FACT LOAD FAILED: Source and ClickHouse counts do not match."
        )

    orphan_employee = ch_client.execute("""
        SELECT count()
        FROM northwind_dw.fact_employee_territories f
        LEFT JOIN northwind_dw.dim_employees d
            ON f.employee_key = d.employee_key
        WHERE d.employee_key IS NULL
    """)[0][0]

    orphan_territory = ch_client.execute("""
        SELECT count()
        FROM northwind_dw.fact_employee_territories f
        LEFT JOIN northwind_dw.dim_territories d
            ON f.territory_key = d.territory_key
        WHERE d.territory_key IS NULL
    """)[0][0]

    print(f"Orphan employee keys: {orphan_employee}")
    print(f"Orphan territory keys: {orphan_territory}")

    if orphan_employee or orphan_territory:
        raise RuntimeError(
            "FACT LOAD FAILED: Orphan dimension keys detected."
        )

    print("FACT LOAD SUCCESS")
    print("Source and ClickHouse counts match.")
    print("Dimension integrity validation: OK")

    pg_cur.close()
    pg_conn.close()
    print("PostgreSQL connection closed")


if __name__ == "__main__":
    main()
