CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE sales.orders (
    order_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Crucial for CDC: logs previous state on UPDATE/DELETE
ALTER TABLE sales.orders REPLICA IDENTITY FULL;

-- Seed enterprise records
INSERT INTO sales.orders (order_id, customer_id, total_amount, status) VALUES
('ORD-2026-001', 'CUST-ALPHA', 12500.00, 'CONFIRMED'),
('ORD-2026-002', 'CUST-BETA', 4300.50, 'PENDING'),
('ORD-2026-003', 'CUST-GAMMA', 98000.00, 'PROCESSING');