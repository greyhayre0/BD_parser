-- Создаем базу данных для тренировок
CREATE DATABASE shop_train;

-- Подключаемся к новой базе
\c shop_train;

-- Сначала нужно отключиться от базы, которую хотите удалить
\c postgres;  -- или \c template1

-- Теперь удалите базу
DROP DATABASE shop_train;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    amount DECIMAL(10, 2),
    order_date DATE
);


INSERT INTO customers (name) VALUES
('Иван Иванов'),
('Мария Петрова'),
('Алексей Сидоров'),
('Елена Кузнецова'),
('Дмитрий Смирнов');

INSERT INTO orders (customer_id, amount, order_date) VALUES
(1, 500.50, '2025-01-15'),
(1, 700.25, '2025-02-20'),
(2, 200.00, '2025-03-10'),
(2, 300.75, '2025-04-05'),
(3, 1500.00, '2025-01-25'),
(3, 600.30, '2025-05-12'),
(4, 100.50, '2025-06-01');

sudo -u postgres psql -d shop_train

# Напиши запрос, который вернёт имена клиентов и сумму всех их заказов.
SELECT DISTINCT customers.name FROM customers JOIN orders ON customer_id = 

SELECT customers.name, SUM(orders.amount)as total FROM customers 
LEFT JOIN orders ON customer_id = customers.id 
GROUP BY customers.id, customers.name
HAVING SUM(orders.amount) IS NOT NULL
ORDER BY total DESC;
LIMIT 2;

SELECT customers.name, SUM(orders.amount) as total FROM customers LEFT JOIN orders ON customer_id = customers.id GROUP BY customer_id, customers.name;

SELECT customers.name, orders.amount, orders.order_date FROM customers JOIN orders ON customers.id = orders.customer_id;

SELECT customers.name, orders.amount, orders.order_date, SUM(orders.amount) OVER (PARTITION BY customers.id) as total_per_customer FROM customers JOIN orders ON customers.id = orders.customer_id ORDER BY customers.id, orders.order_date;

SELECT customers.name, orders.amount FROM customers, orders ORDER BY orders.amount DESC;

SELECT customers.name, SUM(orders.amount) as total FROM customers LEFT JOIN orders ON customer_id = customers.id GROUP BY customer_id, customers.name HAVING SUM(orders.amount) IS NOT NULL ORDER BY total DESC;