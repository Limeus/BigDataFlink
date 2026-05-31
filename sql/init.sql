CREATE SCHEMA IF NOT EXISTS star;

CREATE TABLE IF NOT EXISTS star.dim_customer (
    customer_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_seller (
    seller_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_seller_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_supplier (
    supplier_key TEXT PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_product (
    product_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_product_id INTEGER,
    supplier_key TEXT REFERENCES star.dim_supplier (supplier_key),
    name TEXT,
    category TEXT,
    unit_price NUMERIC(14, 2),
    available_quantity INTEGER,
    pet_category TEXT,
    weight NUMERIC(12, 2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(4, 2),
    review_count INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE IF NOT EXISTS star.dim_store (
    store_key TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_date (
    date_key DATE PRIMARY KEY,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS star.fact_sales (
    sale_event_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_record_number BIGINT,
    source_sale_id INTEGER,
    sale_date DATE REFERENCES star.dim_date (date_key),
    customer_key TEXT REFERENCES star.dim_customer (customer_key),
    seller_key TEXT REFERENCES star.dim_seller (seller_key),
    product_key TEXT REFERENCES star.dim_product (product_key),
    store_key TEXT REFERENCES star.dim_store (store_key),
    supplier_key TEXT REFERENCES star.dim_supplier (supplier_key),
    quantity INTEGER,
    total_price NUMERIC(14, 2),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_date ON star.fact_sales (sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key ON star.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON star.fact_sales (product_key);
