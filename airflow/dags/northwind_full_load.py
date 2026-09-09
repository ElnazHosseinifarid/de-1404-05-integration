import os
import subprocess
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_script(script_name, env_extra=None):
    env = os.environ.copy()

    if env_extra:
        env.update(env_extra)

    script_path = os.path.expanduser(f"~/{script_name}")

    result = subprocess.run(
        ["python", script_path],
        env=env,
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Script failed: {script_name}"
        )

    print(f"SUCCESS: {script_name}")


def run_full_load():
    password = subprocess.check_output(
        ["docker", "exec", "sql_server_sales", "printenv", "SA_PASSWORD"],
        text=True
    ).strip()

    run_script(
        "full_load_sqlserver_to_postgres.py",
        {
            "MSSQL_SA_PASSWORD": password,
            "PG_PASSWORD": "admin123",
        }
    )


def run_customer_scd2():
    run_script("scd2_dim_customer.py")


def run_employees_scd2():
    run_script("scd2_dim_employees.py")


def run_products_scd2():
    run_script("scd2_dim_products.py")


def run_suppliers_scd2():
    run_script("scd2_dim_suppliers.py")


def run_territories_scd2():
    run_script("scd2_dim_territories.py")


def run_fact_orders():
    run_script("load_fact_orders_to_clickhouse.py")


def run_fact_employee_territories():
    run_script("load_fact_employee_territories_to_clickhouse.py")


def validate_dw():
    run_script("validate_northwind_dw.py")


tehran = pendulum.timezone("Asia/Tehran")


with DAG(
    dag_id="northwind_full_load",
    start_date=pendulum.datetime(2026, 1, 1, tz=tehran),
    schedule="0 22 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["Northwind", "Full Load", "ClickHouse", "SCD2"],
) as dag:

    full_load = PythonOperator(
        task_id="sqlserver_to_postgres_full_load",
        python_callable=run_full_load,
    )

    customer_scd2 = PythonOperator(
        task_id="scd2_dim_customer",
        python_callable=run_customer_scd2,
    )

    employees_scd2 = PythonOperator(
        task_id="scd2_dim_employees",
        python_callable=run_employees_scd2,
    )

    products_scd2 = PythonOperator(
        task_id="scd2_dim_products",
        python_callable=run_products_scd2,
    )

    suppliers_scd2 = PythonOperator(
        task_id="scd2_dim_suppliers",
        python_callable=run_suppliers_scd2,
    )

    territories_scd2 = PythonOperator(
        task_id="scd2_dim_territories",
        python_callable=run_territories_scd2,
    )

    fact_orders = PythonOperator(
        task_id="load_fact_orders",
        python_callable=run_fact_orders,
    )

    fact_employee_territories = PythonOperator(
        task_id="load_fact_employee_territories",
        python_callable=run_fact_employee_territories,
    )

    validation = PythonOperator(
        task_id="validate_dw",
        python_callable=validate_dw,
    )

    (
        full_load
        >> customer_scd2
        >> employees_scd2
        >> products_scd2
        >> suppliers_scd2
        >> territories_scd2
        >> fact_orders
        >> fact_employee_territories
        >> validation
    )
