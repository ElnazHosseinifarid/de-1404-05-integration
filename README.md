# Northwind Data Engineering Project

## معرفی

این پروژه یک Data Pipeline برای انتقال و پردازش داده‌های **Northwind** است.

داده‌ها از **SQL Server** استخراج شده، در **PostgreSQL Staging** قرار گرفته و پس از پردازش در **ClickHouse Data Warehouse** ذخیره می‌شوند. مدیریت Workflow با **Apache Airflow** و Visualization با **Grafana** انجام شده است.

## معماری

```text
SQL Server
    ↓
PostgreSQL Staging
    ↓
ETL / Transformation
    ↓
ClickHouse Data Warehouse
    ↓
Grafana
```

## تکنولوژی‌ها

* SQL Server
* PostgreSQL
* ClickHouse
* Python
* Apache Airflow
* Docker
* Grafana

## قابلیت‌های اصلی

* انتقال Full Load از SQL Server به PostgreSQL
* ایجاد Data Warehouse در ClickHouse
* پیاده‌سازی SCD Type 2 برای Dimensionها
* بارگذاری Factها
* اعتبارسنجی داده‌ها
* مدیریت Workflow با Airflow
* ایجاد داشبوردهای تحلیلی در Grafana

## ساختار پروژه

```text
de-1404-05-integration/
├── airflow/
│   └── dags/
├── scripts/
├── sql/
├── .env.example
├── .gitignore
└── docker-compose.yml
```

## اجرای پروژه

ابتدا Repository را دریافت کنید:

```bash
git clone https://github.com/ElnazHosseinifarid/de-1404-05-integration.git
cd de-1404-05-integration
```

سپس سرویس‌ها را با Docker اجرا کرده و فرآیند ETL را از طریق Airflow اجرا کنید.

## Data Warehouse

پایگاه داده Data Warehouse در ClickHouse با نام:

```text
northwind_dw
```

ایجاد شده و شامل جداول Dimension و Fact است.

جدول `FactOrders` شامل **2154 رکورد سفارش** است.

## امنیت

اطلاعات حساس و Passwordها در Repository قرار نگرفته‌اند و فایل `.env` توسط `.gitignore` از Git خارج شده است.
