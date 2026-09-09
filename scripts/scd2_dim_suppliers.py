from datetime import datetime, date

import psycopg2
from clickhouse_driver import Client


PG_HOST = "127.0.0.1"
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


def insert_suppliers(ch_client, rows):
    if not rows:
        return

    ch_client.execute(
        """
        INSERT INTO northwind_dw.dim_suppliers
        (
            supplier_key,
            supplier_alternate_key,
            geography_key,
            company_name,
            contact_name,
            contact_title,
            phone,
            fax,
            homepage,
            start_date,
            end_date,
            is_current,
            version
        )
        VALUES
        """,
        rows,
    )


def update_control(ch_client, run_time):
    ch_client.execute(
        """
        ALTER TABLE northwind_dw.scd2_control
        DELETE WHERE table_name = 'dim_suppliers'
        """,
        settings={"mutations_sync": 1},
    )

    ch_client.execute(
        """
        INSERT INTO northwind_dw.scd2_control
        (table_name, last_run)
        VALUES
        """,
        [("dim_suppliers", run_time)],
    )


def report(ch_client):
    result = ch_client.execute(
        """
        SELECT
            count(),
            countIf(is_current = 1),
            countIf(is_current = 0),
            countIf(is_current = 1 AND end_date IS NULL),
            countIf(is_current = 1 AND end_date IS NOT NULL)
        FROM northwind_dw.dim_suppliers
        """
    )[0]

    print()
    print("========== SCD2 CONTROL ==========")
    print(f"Total rows       : {result[0]}")
    print(f"Current rows     : {result[1]}")
    print(f"Historical rows  : {result[2]}")
    print(f"Valid current    : {result[3]}")
    print(f"Invalid current  : {result[4]}")
    print("===================================")


def main():

    run_time = datetime.now().replace(microsecond=0)

    print("SCD2 dim_suppliers started")
    print("Run time:", run_time)

    # --------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    pg_cur = pg_conn.cursor()

    print("PostgreSQL connection: OK")

    pg_cur.execute(
        """
        SELECT
            supplierkey,
            supplieralternatekey,
            geographykey,
            companyname,
            contactname,
            contacttitle,
            phone,
            fax,
            homepage,
            startdate
        FROM staging.dimsuppliers
        ORDER BY
            supplieralternatekey,
            startdate,
            supplierkey
        """
    )

    source_rows = [
        tuple(normalize_value(v) for v in row)
        for row in pg_cur.fetchall()
    ]

    print("Source rows from PostgreSQL:", len(source_rows))

    # --------------------------------------------------
    # ClickHouse
    # --------------------------------------------------

    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    ch_client.execute("SELECT 1")

    print("ClickHouse connection: OK")

    current_rows = ch_client.execute(
        """
        SELECT
            supplier_key,
            supplier_alternate_key,
            geography_key,
            company_name,
            contact_name,
            contact_title,
            phone,
            fax,
            homepage,
            start_date,
            end_date,
            is_current,
            version
        FROM northwind_dw.dim_suppliers
        WHERE is_current = 1
        """
    )

    current_rows = [
        tuple(normalize_value(v) for v in row)
        for row in current_rows
    ]

    print("Current rows in ClickHouse:", len(current_rows))

    # ==================================================
    # INITIAL LOAD
    # ==================================================

    if len(current_rows) == 0:

        print()
        print("No current suppliers found.")
        print("Performing initial SCD2 load with full source history.")

        grouped = {}

        for source in source_rows:
            business_key = source[1]

            grouped.setdefault(
                business_key,
                []
            ).append(source)

        next_supplier_key = 1
        insert_data = []

        for business_key in sorted(grouped):

            versions = sorted(
                grouped[business_key],
                key=lambda row: (row[9], row[0])
            )

            for index, source in enumerate(versions):

                if index < len(versions) - 1:
                    end_date = versions[index + 1][9]
                    is_current = 0
                else:
                    end_date = None
                    is_current = 1

                insert_data.append(
                    (
                        next_supplier_key,
                        source[1],
                        source[2],
                        source[3],
                        source[4],
                        source[5],
                        source[6],
                        source[7],
                        source[8],
                        source[9],
                        end_date,
                        is_current,
                        index + 1,
                    )
                )

                next_supplier_key += 1

        insert_suppliers(
            ch_client,
            insert_data
        )

        print(
            "Inserted initial SCD2 rows:",
            len(insert_data)
        )

        update_control(
            ch_client,
            run_time
        )

        print("SCD2 control table updated")

        report(ch_client)

        pg_cur.close()
        pg_conn.close()

        return

    # ==================================================
    # INCREMENTAL LOAD
    # ==================================================

    current_by_business_key = {
        row[1]: row
        for row in current_rows
    }

    latest_source = {}

    for source in source_rows:

        business_key = source[1]

        if business_key not in latest_source:

            latest_source[business_key] = source

        else:

            old = latest_source[business_key]

            if (
                source[9] > old[9]
                or (
                    source[9] == old[9]
                    and source[0] > old[0]
                )
            ):
                latest_source[business_key] = source

    print(
        "Latest source business keys:",
        len(latest_source)
    )

    new_rows = []
    changed_rows = []

    for business_key, source in latest_source.items():

        current = current_by_business_key.get(
            business_key
        )

        if current is None:

            new_rows.append(source)
            continue

        source_values = (
            source[2],
            source[3],
            source[4],
            source[5],
            source[6],
            source[7],
            source[8],
        )

        current_values = (
            current[2],
            current[3],
            current[4],
            current[5],
            current[6],
            current[7],
            current[8],
        )

        if source_values != current_values:

            changed_rows.append(
                (source, current)
            )

    print("New rows:", len(new_rows))
    print("Changed rows:", len(changed_rows))

    # --------------------------------------------------
    # New suppliers
    # --------------------------------------------------

    if new_rows:

        next_key = ch_client.execute(
            """
            SELECT
                coalesce(max(supplier_key), 0) + 1
            FROM northwind_dw.dim_suppliers
            """
        )[0][0]

        insert_data = []

        for source in new_rows:

            insert_data.append(
                (
                    next_key,
                    source[1],
                    source[2],
                    source[3],
                    source[4],
                    source[5],
                    source[6],
                    source[7],
                    source[8],
                    source[9],
                    None,
                    1,
                    1,
                )
            )

            next_key += 1

        insert_suppliers(
            ch_client,
            insert_data
        )

        print(
            "Inserted new suppliers:",
            len(insert_data)
        )

    # --------------------------------------------------
    # Changed suppliers
    # --------------------------------------------------

    if changed_rows:

        next_key = ch_client.execute(
            """
            SELECT
                coalesce(max(supplier_key), 0) + 1
            FROM northwind_dw.dim_suppliers
            """
        )[0][0]

        for source, current in changed_rows:

            ch_client.execute(
                """
                ALTER TABLE northwind_dw.dim_suppliers
                UPDATE
                    end_date = %(end_date)s,
                    is_current = 0
                WHERE supplier_key = %(supplier_key)s
                  AND is_current = 1
                """,
                {
                    "end_date": source[9],
                    "supplier_key": current[0],
                },
                settings={"mutations_sync": 1},
            )

            insert_suppliers(
                ch_client,
                [
                    (
                        next_key,
                        source[1],
                        source[2],
                        source[3],
                        source[4],
                        source[5],
                        source[6],
                        source[7],
                        source[8],
                        source[9],
                        None,
                        1,
                        current[12] + 1,
                    )
                ],
            )

            next_key += 1

        print(
            "Closed old SCD2 versions:",
            len(changed_rows)
        )

        print(
            "Inserted new SCD2 versions:",
            len(changed_rows)
        )

    else:

        print("No changed suppliers")

    update_control(
        ch_client,
        run_time
    )

    print("SCD2 control table updated")

    report(ch_client)

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
