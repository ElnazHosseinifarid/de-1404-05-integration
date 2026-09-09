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

    if isinstance(value, memoryview):
        return bytes(value)

    return value


def normalize_row(row):
    values = list(normalize_value(v) for v in row)

    # PostgreSQL birthdate is a timestamp, but ClickHouse stores
    # historical birth dates as Date32 to support dates before 1970.
    if values[9] is not None and isinstance(values[9], datetime):
        values[9] = values[9].date()

    return tuple(values)


def main():

    run_time = datetime.now().replace(microsecond=0)

    print("SCD2 dim_employees started")
    print("Run time:", run_time)

    # PostgreSQL
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
            employeekey,
            parentemployeekey,
            employeealternatekey,
            reportsto,
            geographykey,
            firstname,
            lastname,
            title,
            titleofcourtesy,
            birthdate,
            hiredate,
            homephone,
            extension,
            photo,
            notes,
            photopath
        FROM staging.dimemployees
        ORDER BY employeekey
    """)

    source_rows = [
        normalize_row(row)
        for row in pg_cur.fetchall()
    ]

    print("Source rows from PostgreSQL:", len(source_rows))

    # ClickHouse
    ch_client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

    ch_client.execute("SELECT 1")

    print("ClickHouse connection: OK")

    current_rows = ch_client.execute("""
        SELECT
            employee_key,
            parent_employee_key,
            employee_alternate_key,
            reports_to,
            geography_key,
            first_name,
            last_name,
            title,
            title_of_courtesy,
            birth_date,
            hire_date,
            home_phone,
            extension,
            photo,
            notes,
            photo_path,
            start_date,
            end_date,
            is_current,
            version
        FROM dim_employees
        WHERE is_current = 1
    """)

    current_rows = [
        normalize_row(row)
        for row in current_rows
    ]

    print("Current rows in ClickHouse:", len(current_rows))

    current_by_business_key = {
        row[2]: row
        for row in current_rows
    }

    max_key_result = ch_client.execute("""
        SELECT coalesce(max(employee_key), 0)
        FROM dim_employees
    """)

    next_employee_key = max_key_result[0][0] + 1

    new_rows = []
    changed_rows = []
    unchanged_count = 0

    for source in source_rows:

        business_key = source[2]

        current = current_by_business_key.get(business_key)

        if current is None:
            new_rows.append(source)
            continue

        # Source attributes:
        # parent_employee_key
        # employee_alternate_key
        # reports_to
        # geography_key
        # first_name
        # last_name
        # title
        # title_of_courtesy
        # birth_date
        # hire_date
        # home_phone
        # extension
        # photo
        # notes
        # photo_path

        source_attributes = source[1:16]

        # Same attributes from ClickHouse
        current_attributes = current[1:16]

        if source_attributes != current_attributes:
            changed_rows.append((source, current))
        else:
            unchanged_count += 1

    print("New rows:", len(new_rows))
    print("Changed rows:", len(changed_rows))
    print("Unchanged rows:", unchanged_count)

    # ---------------------------------------------------------
    # INSERT NEW EMPLOYEES
    # ---------------------------------------------------------

    if new_rows:

        insert_data = []

        for source in new_rows:

            insert_data.append(
                (
                    next_employee_key,
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
                    source[12],
                    source[13],
                    source[14],
                    source[15],
                    run_time,
                    None,
                    1,
                    1,
                )
            )

            next_employee_key += 1

        ch_client.execute(
            """
            INSERT INTO dim_employees
            (
                employee_key,
                parent_employee_key,
                employee_alternate_key,
                reports_to,
                geography_key,
                first_name,
                last_name,
                title,
                title_of_courtesy,
                birth_date,
                hire_date,
                home_phone,
                extension,
                photo,
                notes,
                photo_path,
                start_date,
                end_date,
                is_current,
                version
            )
            VALUES
            """,
            insert_data,
        )

        print("Inserted new employees:", len(insert_data))

    else:
        print("No new employees")

    # ---------------------------------------------------------
    # CLOSE OLD SCD2 VERSIONS
    # ---------------------------------------------------------

    if changed_rows:

        for source, current in changed_rows:

            old_employee_key = current[0]

            ch_client.execute(
                """
                ALTER TABLE dim_employees
                UPDATE
                    end_date = %(end_date)s,
                    is_current = 0
                WHERE employee_key = %(employee_key)s
                  AND is_current = 1
                """,
                {
                    "end_date": run_time,
                    "employee_key": old_employee_key,
                },
                settings={"mutations_sync": 1},
            )

        print("Closed old SCD2 versions:", len(changed_rows))

        # -----------------------------------------------------
        # INSERT NEW VERSIONS
        # -----------------------------------------------------

        insert_data = []

        for source, current in changed_rows:

            insert_data.append(
                (
                    next_employee_key,
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
                    source[12],
                    source[13],
                    source[14],
                    source[15],
                    run_time,
                    None,
                    1,
                    current[19] + 1,
                )
            )

            next_employee_key += 1

        ch_client.execute(
            """
            INSERT INTO dim_employees
            (
                employee_key,
                parent_employee_key,
                employee_alternate_key,
                reports_to,
                geography_key,
                first_name,
                last_name,
                title,
                title_of_courtesy,
                birth_date,
                hire_date,
                home_phone,
                extension,
                photo,
                notes,
                photo_path,
                start_date,
                end_date,
                is_current,
                version
            )
            VALUES
            """,
            insert_data,
        )

        print("Inserted new SCD2 versions:", len(insert_data))

    else:
        print("No changed employees")

    # ---------------------------------------------------------
    # UPDATE CONTROL TABLE
    # ---------------------------------------------------------

    ch_client.execute(
        """
        ALTER TABLE scd2_control
        DELETE WHERE table_name = 'dim_employees'
        """,
        settings={"mutations_sync": 1},
    )

    ch_client.execute(
        """
        INSERT INTO scd2_control
        (table_name, last_run)
        VALUES
        """,
        [("dim_employees", run_time)],
    )

    print("SCD2 control table updated")

    # ---------------------------------------------------------
    # FINAL VALIDATION
    # ---------------------------------------------------------

    total_rows = ch_client.execute(
        "SELECT count() FROM dim_employees"
    )[0][0]

    current_count = ch_client.execute(
        """
        SELECT count()
        FROM dim_employees
        WHERE is_current = 1
        """
    )[0][0]

    historical_count = ch_client.execute(
        """
        SELECT count()
        FROM dim_employees
        WHERE is_current = 0
        """
    )[0][0]

    duplicate_current = ch_client.execute(
        """
        SELECT count()
        FROM
        (
            SELECT employee_alternate_key
            FROM dim_employees
            WHERE is_current = 1
            GROUP BY employee_alternate_key
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

    pg_cur.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
