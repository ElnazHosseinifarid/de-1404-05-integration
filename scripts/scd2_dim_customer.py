from datetime import datetime

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


def main():
    run_time = datetime.now().replace(microsecond=0)

    print("SCD2 dim_customer started")
    print(f"Run time: {run_time}")

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    print("PostgreSQL connection: OK")

    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

    ch_client.execute("SELECT 1")

    print("ClickHouse connection: OK")
    # ---------------------------------------------------------
    # 3. Read source data from PostgreSQL Staging
    # ---------------------------------------------------------
    pg_cur = pg_conn.cursor()

    pg_cur.execute(
        """
        SELECT
            customerkey,
            customeralternatekey,
            geographykey,
            companyname,
            contactname,
            contacttitle,
            phone,
            fax
        FROM staging.dimcustomer
        ORDER BY customerkey
        """
    )

    source_rows = pg_cur.fetchall()

    print(f"Source rows from PostgreSQL: {len(source_rows)}")

    # ---------------------------------------------------------
    # 4. Read current rows from ClickHouse
    # ---------------------------------------------------------
    current_rows = ch_client.execute(
        """
        SELECT
            customer_key,
            customer_alternate_key,
            geography_key,
            company_name,
            contact_name,
            contact_title,
            phone,
            fax,
            start_date,
            end_date,
            is_current,
            version
        FROM dim_customer
        WHERE is_current = 1
        """
    )

    print(f"Current rows in ClickHouse: {len(current_rows)}")
    # ---------------------------------------------------------
    # 5. Prepare current dimension lookup
    # ---------------------------------------------------------
    current_by_business_key = {
        row[1]: row
        for row in current_rows
    }

    max_key_result = ch_client.execute(
        "SELECT coalesce(max(customer_key), 0) FROM dim_customer"
    )

    next_customer_key = max_key_result[0][0] + 1

    new_rows = []
    changed_rows = []

    for source in source_rows:
        business_key = source[1]
        current = current_by_business_key.get(business_key)

        if current is None:
            new_rows.append(source)
            continue

        source_attributes = source[2:8]
        current_attributes = current[2:8]

        if source_attributes != current_attributes:
            changed_rows.append((source, current))

    print(f"New rows: {len(new_rows)}")
    print(f"Changed rows: {len(changed_rows)}")
    print(f"Unchanged rows: {len(source_rows) - len(new_rows) - len(changed_rows)}")
    # ---------------------------------------------------------
    # 6. Insert new customers - SCD Type 2 Version 1
    # ---------------------------------------------------------
    if new_rows:
        insert_data = []

        for source in new_rows:
            insert_data.append(
                (
                    next_customer_key,
                    source[1],
                    source[2],
                    source[3],
                    source[4],
                    source[5],
                    source[6],
                    source[7],
                    run_time,
                    None,
                    1,
                    1,
                )
            )

            next_customer_key += 1

        ch_client.execute(
            """
            INSERT INTO dim_customer
            (
                customer_key,
                customer_alternate_key,
                geography_key,
                company_name,
                contact_name,
                contact_title,
                phone,
                fax,
                start_date,
                end_date,
                is_current,
                version
            )
            VALUES
            """,
            insert_data,
        )

        print(f"Inserted new SCD2 rows: {len(insert_data)}")
    else:
        print("No new customers")
    # ---------------------------------------------------------
    # 7. Close old versions for changed customers
    # ---------------------------------------------------------
    if changed_rows:
        for source, current in changed_rows:
            old_customer_key = current[0]

            ch_client.execute(
                """
                ALTER TABLE dim_customer
                UPDATE
                    end_date = %(end_date)s,
                    is_current = 0
                WHERE customer_key = %(customer_key)s
                  AND is_current = 1
                """,
                {
                    "end_date": run_time,
                    "customer_key": old_customer_key,
                },
                settings={"mutations_sync": 1},
            )

        print(f"Closed old SCD2 versions: {len(changed_rows)}")
    else:
        print("No changed customers")
    # ---------------------------------------------------------
    # 8. Insert new versions for changed customers
    # ---------------------------------------------------------
    if changed_rows:
        insert_data = []

        for source, current in changed_rows:
            insert_data.append(
                (
                    next_customer_key,
                    source[1],
                    source[2],
                    source[3],
                    source[4],
                    source[5],
                    source[6],
                    source[7],
                    run_time,
                    None,
                    1,
                    current[11] + 1,
                )
            )

            next_customer_key += 1

        ch_client.execute(
            """
            INSERT INTO dim_customer
            (
                customer_key,
                customer_alternate_key,
                geography_key,
                company_name,
                contact_name,
                contact_title,
                phone,
                fax,
                start_date,
                end_date,
                is_current,
                version
            )
            VALUES
            """,
            insert_data,
        )

        print(f"Inserted new SCD2 versions: {len(insert_data)}")
    else:
        print("No changed customers to version")
    # ---------------------------------------------------------
    # 9. Update SCD2 control table
    # ---------------------------------------------------------
    ch_client.execute(
        """
        ALTER TABLE scd2_control
        DELETE WHERE table_name = 'dim_customer'
        """,
        settings={"mutations_sync": 1},
    )

    ch_client.execute(
        """
        INSERT INTO scd2_control
        (table_name, last_run)
        VALUES
        """,
        [
            ("dim_customer", run_time)
        ],
    )

    print("SCD2 control table updated")

    # ---------------------------------------------------------
    # 10. Final validation
    # ---------------------------------------------------------
    total_rows = ch_client.execute(
        "SELECT count() FROM dim_customer"
    )[0][0]

    current_count = ch_client.execute(
        """
        SELECT count()
        FROM dim_customer
        WHERE is_current = 1
        """
    )[0][0]

    historical_count = ch_client.execute(
        """
        SELECT count()
        FROM dim_customer
        WHERE is_current = 0
        """
    )[0][0]

    duplicate_current = ch_client.execute(
        """
        SELECT count()
        FROM
        (
            SELECT customer_alternate_key
            FROM dim_customer
            WHERE is_current = 1
            GROUP BY customer_alternate_key
            HAVING count() > 1
        )
        """
    )[0][0]

    print("--------------------------------------------------")
    print("SCD2 dim_customer completed")
    print(f"Total rows       : {total_rows}")
    print(f"Current rows     : {current_count}")
    print(f"Historical rows  : {historical_count}")
    print(f"Duplicate current: {duplicate_current}")
    print("--------------------------------------------------")

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
