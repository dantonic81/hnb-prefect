CREATE SCHEMA IF NOT EXISTS data;

CREATE TABLE data.processing_statistics (
    id SERIAL PRIMARY KEY,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    dataset_type VARCHAR(255) NOT NULL,
    record_count INTEGER NOT NULL,
    processing_time INTERVAL NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.customers (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    date_of_birth DATE,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    postcode VARCHAR(20),
    last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    segment VARCHAR(50),
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID UNIQUE NOT NULL,
    transaction_time TIMESTAMP WITH TIME ZONE NOT NULL,
    customer_id INTEGER NOT NULL,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.delivery_addresses (
    id SERIAL PRIMARY KEY,
    transaction_id UUID REFERENCES data.transactions(transaction_id),
    address TEXT NOT NULL,
    postcode TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data.purchases (
    id SERIAL PRIMARY KEY,
    transaction_id UUID REFERENCES data.transactions(transaction_id) ON DELETE CASCADE,
    product_sku INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    total NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS data.products (
    sku INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(50) NOT NULL,
    popularity DOUBLE PRECISION CHECK (popularity > 0) NOT NULL,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.invalid_products (
    id SERIAL PRIMARY KEY,
    sku INTEGER NOT NULL,
    name VARCHAR(255),
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(255) NOT NULL,
    popularity DECIMAL(5, 4) NOT NULL,
    error_message TEXT,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.invalid_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data.invalid_customers (
    id INTEGER PRIMARY KEY,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data.invalid_erasure_requests (
    customer_id INT PRIMARY KEY,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    email VARCHAR(255),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data.erasure_requests (
    customer_id INT PRIMARY KEY,
    record_date DATE NOT NULL,
    record_hour INTEGER NOT NULL,
    email VARCHAR(255),
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);