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
CH_DB = "northwind_dw"


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


def main():

    run_time = datetime.now().replace(microsecond=0)

    print("SCD2 dim_territories started")
    print("Run time:", run_time)

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    pg_cur = pg_conn.cursor()

    print("PostgreSQL connection: OK")

    pg_cur.execute("""
        SELECT
            territorykey,
            territoryalternatekey,
            regiondescription,
            territorydescription,
            startdate,
            enddate
        FROM staging.dimterritories
        ORDER BY territorykey
    """)

    source_rows = [
        tuple(normalize_value(v) for v in row)
        for row in pg_cur.fetchall()
    ]

    print("Source rows from PostgreSQL:", len(source_rows))

    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    ch_client.execute("SELECT 1")

    print("ClickHouse connection: OK")

    current_rows = ch_client.execute("""
        SELECT
            territory_key,
            territory_alternate_key,
            region_description,
            territory_description,
            start_date,
            end_date,
            is_current,
            version
        FROM northwind_dw.dim_territories
        WHERE is_current = 1
    """)

    current_rows = [
        tuple(normalize_value(v) for v in row)
        for row in current_rows
    ]

    print("Current rows in ClickHouse:", len(current_rows))

    current_by_business_key = {
        row[1]: row
        for row in current_rows
    }

    max_key_result = ch_client.execute("""
        SELECT coalesce(max(territory_key), 0)
        FROM northwind_dw.dim_territories
    """)

    next_territory_key = max_key_result[0][0] + 1

    new_rows = []
    changed_rows = []
    unchanged_count = 0

    for source in source_rows:

        business_key = source[1]

        current = current_by_business_key.get(business_key)

        if current is None:
            new_rows.append(source)
            continue

        source_attributes = (
            source[2],
            source[3],
        )

        current_attributes = (
            current[2],
            current[3],
        )

        if source_attributes != current_attributes:
            changed_rows.append((source, current))
        else:
            unchanged_count += 1

    print("New rows:", len(new_rows))
    print("Changed rows:", len(changed_rows))
    print("Unchanged rows:", unchanged_count)

    # New territories
    if new_rows:

        insert_data = []

        for source in new_rows:

            insert_data.append(
                (
                    next_territory_key,
                    source[1],
                    source[2],
                    source[3],
                    run_time,
                    None,
                    1,
                    1,
                )
            )

            next_territory_key += 1

        ch_client.execute(
            """
            INSERT INTO northwind_dw.dim_territories
            (
                territory_key,
                territory_alternate_key,
                region_description,
                territory_description,
                start_date,
                end_date,
                is_current,
                version
            )
            VALUES
            """,
            insert_data,
        )

        print("Inserted new territories:", len(insert_data))

    else:
        print("No new territories")

    # Changed territories
    if changed_rows:

        for source, current in changed_rows:

            old_territory_key = current[0]

            ch_client.execute(
                """
                ALTER TABLE northwind_dw.dim_territories
                UPDATE
                    end_date = %(end_date)s,
                    is_current = 0
                WHERE territory_key = %(territory_key)s
                  AND is_current = 1
                """,
                {
                    "end_date": run_time,
                    "territory_key": old_territory_key,
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
                    next_territory_key,
                    source[1],
                    source[2],
                    source[3],
                    run_time,
                    None,
                    1,
                    current[7] + 1,
                )
            )

            next_territory_key += 1

        ch_client.execute(
            """
            INSERT INTO northwind_dw.dim_territories
            (
                territory_key,
                territory_alternate_key,
                region_description,
                territory_description,
                start_date,
                end_date,
                is_current,
                version
            )
            VALUES
            """,
            insert_data,
        )

        print(
            "Inserted new SCD2 versions:",
            len(insert_data)
        )

    else:
        print("No changed territories")

    # Update SCD2 control
    ch_client.execute(
        """
        ALTER TABLE northwind_dw.scd2_control
        DELETE WHERE table_name = 'dim_territories'
        """,
        settings={"mutations_sync": 1},
    )

    ch_client.execute(
        """
        INSERT INTO northwind_dw.scd2_control
        (table_name, last_run)
        VALUES
        """,
        [("dim_territories", run_time)],
    )

    print("SCD2 control table updated")

    # Final control
    control = ch_client.execute("""
        SELECT
            count(),
            countIf(is_current = 1),
            countIf(is_current = 0),
            countIf(is_current = 1 AND end_date IS NULL),
            countIf(is_current = 1 AND end_date IS NOT NULL)
        FROM northwind_dw.dim_territories
    """)[0]

    duplicate_current = ch_client.execute("""
        SELECT count()
        FROM
        (
            SELECT territory_alternate_key
            FROM northwind_dw.dim_territories
            WHERE is_current = 1
            GROUP BY territory_alternate_key
            HAVING count() > 1
        )
    """)[0][0]

    print()
    print("========== SCD2 CONTROL ==========")
    print("Total rows       :", control[0])
    print("Current rows     :", control[1])
    print("Historical rows  :", control[2])
    print("Valid current    :", control[3])
    print("Invalid current  :", control[4])
    print("Duplicate current:", duplicate_current)
    print("===================================")

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
