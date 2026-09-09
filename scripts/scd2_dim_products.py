from datetime import datetime, date

import psycopg2
from clickhouse_driver import Client


PG_HOST = "localhost"
PG_PORT = 25432
PG_DB = "sales_products"
PG_USER = "admin"
PG_PASSWORD = "admin123"

CH_HOST = "127.0.0.1"
CH_PORT = 29000
CH_USER = "default"
CH_PASSWORD = "admin123"
CH_DATABASE = "northwind_dw"


def normalize_value(value):
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, datetime.min.time())

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, memoryview):
        return bytes(value)

    return value


def normalize_row(row):
    return tuple(normalize_value(v) for v in row)


def insert_products(ch_client, insert_data):
    if not insert_data:
        return

    ch_client.execute(
        """
        INSERT INTO dim_products
        (
            product_key,
            product_alternate_key,
            supplier_key,
            product_name,
            category_name,
            quantity_per_unit,
            unit_price,
            units_in_stock,
            units_on_order,
            reorder_level,
            discontinued,
            start_date,
            end_date,
            is_current,
            version
        )
        VALUES
        """,
        insert_data,
    )


def print_control_report(ch_client):
    total_rows = ch_client.execute(
        "SELECT count() FROM dim_products"
    )[0][0]

    current_count = ch_client.execute(
        """
        SELECT count()
        FROM dim_products
        WHERE is_current = 1
        """
    )[0][0]

    historical_count = ch_client.execute(
        """
        SELECT count()
        FROM dim_products
        WHERE is_current = 0
        """
    )[0][0]

    duplicate_current = ch_client.execute(
        """
        SELECT count()
        FROM
        (
            SELECT product_alternate_key
            FROM dim_products
            WHERE is_current = 1
            GROUP BY product_alternate_key
            HAVING count() > 1
        )
        """
    )[0][0]

    print()
    print("========== SCD2 CONTROL ==========")
    print("Total rows       :", total_rows)
    print("Current rows     :", current_count)
    print("Historical rows  :", historical_count)
    print("Duplicate current:", duplicate_current)
    print("===================================")


def update_control_table(ch_client, run_time):
    ch_client.execute(
        """
        ALTER TABLE scd2_control
        DELETE WHERE table_name = 'dim_products'
        """,
        settings={"mutations_sync": 1},
    )

    ch_client.execute(
        """
        INSERT INTO scd2_control
        (table_name, last_run)
        VALUES
        """,
        [("dim_products", run_time)],
    )

    print("SCD2 control table updated")


def main():

    run_time = datetime.now().replace(microsecond=0)

    print("SCD2 dim_products started")
    print("Run time:", run_time)

    # ---------------------------------------------------------
    # PostgreSQL
    # ---------------------------------------------------------

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    pg_cur = pg_conn.cursor()

    print("PostgreSQL connection: OK")

    pg_cur.execute(
        """
        SELECT
            productkey,
            productalternatekey,
            supplierkey,
            productname,
            categoryname,
            quantityperunit,
            unitprice,
            unitsinstock,
            unitsonorder,
            reorderlevel,
            discontinued,
            startdate
        FROM staging.dimproducts
        ORDER BY productalternatekey, startdate, productkey
        """
    )

    source_rows = [
        normalize_row(row)
        for row in pg_cur.fetchall()
    ]

    print("Source rows from PostgreSQL:", len(source_rows))

    # ---------------------------------------------------------
    # ClickHouse
    # ---------------------------------------------------------

    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

    ch_client.execute("SELECT 1")

    print("ClickHouse connection: OK")

    current_rows = ch_client.execute(
        """
        SELECT
            product_key,
            product_alternate_key,
            supplier_key,
            product_name,
            category_name,
            quantity_per_unit,
            unit_price,
            units_in_stock,
            units_on_order,
            reorder_level,
            discontinued,
            start_date,
            end_date,
            is_current,
            version
        FROM dim_products
        WHERE is_current = 1
        """
    )

    current_rows = [
        normalize_row(row)
        for row in current_rows
    ]

    print("Current rows in ClickHouse:", len(current_rows))

    max_key_result = ch_client.execute(
        """
        SELECT coalesce(max(product_key), 0)
        FROM dim_products
        """
    )

    next_product_key = max_key_result[0][0] + 1

    # =========================================================
    # INITIAL LOAD
    # =========================================================

    if not current_rows:

        print()
        print("No current products found.")
        print("Performing initial SCD2 load with full source history.")

        grouped_source = {}

        for row in source_rows:
            business_key = row[1]
            grouped_source.setdefault(business_key, []).append(row)

        initial_insert_data = []

        for business_key in sorted(grouped_source):

            versions = sorted(
                grouped_source[business_key],
                key=lambda row: (row[11], row[0]),
            )

            version_count = len(versions)

            for index, source in enumerate(versions):

                is_current = 1 if index == version_count - 1 else 0

                if is_current:
                    end_date = None
                else:
                    end_date = versions[index + 1][11]

                initial_insert_data.append(
                    (
                        next_product_key,
                        source[1],
                        source[2],
                        source[3],
                        source[4],
                        source[5],
                        source[6],
                        source[7],
                        source[8],
                        source[9],
                        source[10],
                        source[11],
                        end_date,
                        is_current,
                        index + 1,
                    )
                )

                next_product_key += 1

        insert_products(ch_client, initial_insert_data)

        print(
            "Inserted initial SCD2 rows:",
            len(initial_insert_data)
        )

        update_control_table(ch_client, run_time)
        print_control_report(ch_client)

        pg_cur.close()
        pg_conn.close()
        return

    # =========================================================
    # INCREMENTAL SCD2 LOAD
    # =========================================================

    current_by_business_key = {
        row[1]: row
        for row in current_rows
    }

    # ---------------------------------------------------------
    # Select latest source version per business key
    # ---------------------------------------------------------

    latest_source_by_business_key = {}

    for row in source_rows:

        business_key = row[1]
        source_start_date = row[11]

        existing = latest_source_by_business_key.get(business_key)

        if existing is None:
            latest_source_by_business_key[business_key] = row
        else:
            existing_start_date = existing[11]

            if (
                source_start_date > existing_start_date
                or (
                    source_start_date == existing_start_date
                    and row[0] > existing[0]
                )
            ):
                latest_source_by_business_key[business_key] = row

    latest_source_rows = list(
        latest_source_by_business_key.values()
    )

    print(
        "Latest source business keys:",
        len(latest_source_rows)
    )

    new_rows = []
    changed_rows = []
    unchanged_count = 0

    # ---------------------------------------------------------
    # Compare latest source with current DW versions
    # ---------------------------------------------------------

    for source in latest_source_rows:

        business_key = source[1]

        current = current_by_business_key.get(business_key)

        if current is None:
            new_rows.append(source)
            continue

        source_attributes = source[2:11]
        current_attributes = current[2:11]

        if source_attributes != current_attributes:
            changed_rows.append((source, current))
        else:
            unchanged_count += 1

    print("New rows:", len(new_rows))
    print("Changed rows:", len(changed_rows))
    print("Unchanged rows:", unchanged_count)

    # ---------------------------------------------------------
    # Insert completely new products
    # ---------------------------------------------------------

    if new_rows:

        insert_data = []

        for source in new_rows:

            insert_data.append(
                (
                    next_product_key,
                    source[1],
                    source[2],
                    source[3],
                    source[4],
                    source[5],
                    source[6],
                    source[7],
                    source[8],
                    source[9],
                    source[10],
                    source[11],
                    None,
                    1,
                    1,
                )
            )

            next_product_key += 1

        insert_products(ch_client, insert_data)

        print(
            "Inserted new products:",
            len(insert_data)
        )

    else:
        print("No new products")

    # ---------------------------------------------------------
    # SCD Type 2 changes
    # ---------------------------------------------------------

    if changed_rows:

        for source, current in changed_rows:

            old_product_key = current[0]
            new_start_date = source[11]

            ch_client.execute(
                """
                ALTER TABLE dim_products
                UPDATE
                    end_date = %(end_date)s,
                    is_current = 0
                WHERE product_key = %(product_key)s
                  AND is_current = 1
                """,
                {
                    "end_date": new_start_date,
                    "product_key": old_product_key,
                },
                settings={"mutations_sync": 1},
            )

        print(
            "Closed old SCD2 versions:",
            len(changed_rows)
        )

        insert_data = []

        for source, current in changed_rows:

            insert_data.append(
                (
                    next_product_key,
                    source[1],
                    source[2],
                    source[3],
                    source[4],
                    source[5],
                    source[6],
                    source[7],
                    source[8],
                    source[9],
                    source[10],
                    source[11],
                    None,
                    1,
                    current[14] + 1,
                )
            )

            next_product_key += 1

        insert_products(ch_client, insert_data)

        print(
            "Inserted new SCD2 versions:",
            len(insert_data)
        )

    else:
        print("No changed products")

    # ---------------------------------------------------------
    # SCD2 control
    # ---------------------------------------------------------

    update_control_table(ch_client, run_time)

    # ---------------------------------------------------------
    # Control report
    # ---------------------------------------------------------

    print_control_report(ch_client)

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
