
DROP TABLE IF EXISTS sales;

CREATE TABLE sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    product_id INT,
    product_name VARCHAR(100),
    quantity_sold INT,
    unit_price DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    customer_id INT,
    customer_name VARCHAR(100),
    customer_phone VARCHAR(20),
    customer_email VARCHAR(100),
    sales_rep_id INT,
    sales_rep_name VARCHAR(100),
    region VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    notes TEXT
);

INSERT INTO sales (date, product_id, product_name, quantity_sold, unit_price, total_price,
                   customer_id, customer_name, customer_phone, customer_email,
                   sales_rep_id, sales_rep_name, region, city, state, country,
                   payment_method, payment_status, notes)
VALUES
('2024-06-01', 1, 'Mobile Phone A', 10, 500.00, 5000.00,
 101, 'John Doe', '123-456-7890', 'john.doe@example.com',
 201, 'Jane Smith', 'North', 'New York', 'NY', 'USA',
 'Credit Card', 'Paid', 'Customer is a VIP'),

('2024-06-02', 2, 'Mobile Phone B', 5, 700.00, 3500.00,
 102, 'Alice Johnson', '987-654-3210', 'alice.johnson@example.com',
 202, 'David Brown', 'South', 'Los Angeles', 'CA', 'USA',
 'PayPal', 'Paid', 'Shipped via express delivery'),

('2024-06-03', 3, 'Mobile Phone C', 8, 600.00, 4800.00,
 103, 'Michael Wilson', '555-123-4567', 'michael.wilson@example.com',
 203, 'Emma Davis', 'West', 'San Francisco', 'CA', 'USA',
 'Bank Transfer', 'Pending', 'Contact customer for payment confirmation');
