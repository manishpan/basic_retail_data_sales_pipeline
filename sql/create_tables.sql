CREATE TABLE IF NOT EXISTS monthly_sales_by_location(
    location VARCHAR2(20),
    month INTEGER,
    total_revenue DECIMAL
);

CREATE TABLE IF NOT EXISTS monthly_sales_by_stores(
    store_name VARCHAR2(20),
    month INTEGER,
    total_revenue DECIMAL
);

CREATE TABLE IF NOT EXISTS monthly_sales_by_product(
    name VARCHAR2(20),
    month INTEGER,
    units_sold INTEGER,
    total_revenue DECIMAL
);

CREATE TABLE IF NOT EXISTS monthly_sales_by_category(
    category VARCHAR2(20),
    month INTEGER,
    units_sold INTEGER,
    total_revenue DECIMAL
);

CREATE TABLE IF NOT EXISTS sales_by_month(
    month INTEGER,
    transaction_count INTEGER,
    quantity INTEGER,
    total_price DECIMAL(5, 2),
    quantity_per_transaction DECIMAL,
    items_price_per_transaction DECIMAL
);

CREATE TABLE IF NOT EXISTS average_basket_size(
    month INTEGER,
    avg_monthly_basket_value DECIMAL
);