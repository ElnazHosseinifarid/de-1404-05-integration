-- Northwind DW - ClickHouse
-- Layer: Data Warehouse

CREATE TABLE IF NOT EXISTS northwind_dw.dim_customer
(
    customer_key Int32,
    customer_alternate_key Nullable(String),
    geography_key Nullable(Int32),
    company_name Nullable(String),
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),

    start_date DateTime,
    end_date Nullable(DateTime),

    is_current UInt8 DEFAULT 1,
    version UInt32 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY customer_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_date
(
    date_key Int32,
    full_date_alternate_key Date,
    calendar_year Int16,
    calendar_season Int16,
    season_name Nullable(String),
    month_number_of_year Int16,
    month_name String,
    day_number_of_month Nullable(Int16),
    day_of_week Int16,
    day_of_week_name String
)
ENGINE = MergeTree
ORDER BY date_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_employees
(
    employee_key Int32,
    parent_employee_key Nullable(Int32),
    employee_alternate_key Nullable(Int32),
    reports_to Nullable(Int32),
    geography_key Nullable(Int32),
    first_name Nullable(String),
    last_name Nullable(String),
    title Nullable(String),
    title_of_courtesy Nullable(String),
    birth_date Nullable(DateTime),
    hire_date Nullable(DateTime),
    home_phone Nullable(String),
    extension Nullable(String),
    photo Nullable(String),
    notes Nullable(String),
    photo_path Nullable(String),

    start_date DateTime,
    end_date Nullable(DateTime),

    is_current UInt8 DEFAULT 1,
    version UInt32 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY employee_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_geography
(
    geography_key Int32,
    country Nullable(String),
    region Nullable(String),
    city Nullable(String),
    postal_code Nullable(String),
    address Nullable(String)
)
ENGINE = MergeTree
ORDER BY geography_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_products
(
    product_key Int32,
    product_alternate_key Int32,
    supplier_key Nullable(Int32),
    product_name Nullable(String),
    category_name Nullable(String),
    quantity_per_unit Nullable(String),
    unit_price Nullable(Decimal(18,2)),
    units_in_stock Nullable(Int16),
    units_on_order Nullable(Int16),
    reorder_level Nullable(Int16),
    discontinued Nullable(UInt8),

    start_date DateTime,
    end_date Nullable(DateTime),

    is_current UInt8 DEFAULT 1,
    version UInt32 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY product_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_shippers
(
    shipper_key Int32,
    shipper_alternate_key Nullable(Int32),
    company_name Nullable(String),
    phone Nullable(String)
)
ENGINE = MergeTree
ORDER BY shipper_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_suppliers
(
    supplier_key Int32,
    supplier_alternate_key Nullable(Int32),
    geography_key Nullable(Int32),
    company_name Nullable(String),
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    homepage Nullable(String),

    start_date Nullable(DateTime),
    end_date Nullable(DateTime),

    is_current UInt8 DEFAULT 1,
    version UInt32 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY supplier_key;


CREATE TABLE IF NOT EXISTS northwind_dw.dim_territories
(
    territory_key Int32,
    territory_alternate_key Nullable(String),
    region_description Nullable(String),
    territory_description Nullable(String),

    start_date DateTime,
    end_date Nullable(DateTime),

    is_current UInt8 DEFAULT 1,
    version UInt32 DEFAULT 1
)
ENGINE = MergeTree
ORDER BY territory_key;


CREATE TABLE IF NOT EXISTS northwind_dw.fact_employee_territories
(
    employee_key Int32,
    territory_key Int32
)
ENGINE = MergeTree
ORDER BY (employee_key, territory_key);


CREATE TABLE IF NOT EXISTS northwind_dw.fact_orders
(
    order_id Int32,
    geography_key Nullable(Int32),
    product_key Int32,
    customer_key Nullable(Int32),
    employee_key Nullable(Int32),
    shipper_key Nullable(Int32),

    order_date_key Nullable(Int32),
    required_date_key Nullable(Int32),
    shipped_date_key Nullable(Int32),

    freight Nullable(Decimal(18,2)),
    ship_name Nullable(String),
    unit_price Nullable(Decimal(18,2)),
    quantity Nullable(Int16),
    discount Nullable(Float32),

    order_date Nullable(DateTime),
    shipped_date Nullable(DateTime),
    required_date Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY order_id;
