-- OLAP Database Schema and Data Inserts
BEGIN;

-- Table: dimdate
DROP TABLE IF EXISTS dimdate CASCADE;
CREATE TABLE dimdate (
    fulldate TIMESTAMP,
    datekey INTEGER PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    dayofweek VARCHAR(255)
);

-- Insert data into dimdate
INSERT INTO dimdate VALUES ('2025-09-27 10:15:00', 1, 2025, 9, 27, 'Saturday');
INSERT INTO dimdate VALUES ('2025-09-27 18:40:00', 2, 2025, 9, 27, 'Saturday');
INSERT INTO dimdate VALUES ('2025-09-26 12:05:00', 3, 2025, 9, 26, 'Friday');
INSERT INTO dimdate VALUES ('2025-09-20 09:30:00', 4, 2025, 9, 20, 'Saturday');
INSERT INTO dimdate VALUES ('2025-08-15 14:20:00', 5, 2025, 8, 15, 'Friday');
INSERT INTO dimdate VALUES ('2025-09-29 16:50:00', 6, 2025, 9, 29, 'Monday');
INSERT INTO dimdate VALUES ('2025-09-30 08:10:00', 7, 2025, 9, 30, 'Tuesday');


-- Table: dimstore
DROP TABLE IF EXISTS dimstore CASCADE;
CREATE TABLE dimstore (
    storekey INTEGER PRIMARY KEY,
    storename VARCHAR(255),
    city VARCHAR(255),
    region VARCHAR(255)
);

-- Insert data into dimstore
INSERT INTO dimstore VALUES (3001, 'Central Market', 'Paris', NULL);
INSERT INTO dimstore VALUES (3002, 'Westside Outlet', 'Lyon', NULL);


-- Table: dimproduct
DROP TABLE IF EXISTS dimproduct CASCADE;
CREATE TABLE dimproduct (
    productkey INTEGER PRIMARY KEY,
    productname VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(18,2),
    brand VARCHAR(255)
);

-- Insert data into dimproduct
INSERT INTO dimproduct VALUES (5001, 'Coffee', 'Beverages', 6.5, NULL);
INSERT INTO dimproduct VALUES (5002, 'Tea', 'Beverages', 4.0, NULL);
INSERT INTO dimproduct VALUES (5003, 'Chips', 'Snacks', 2.5, NULL);
INSERT INTO dimproduct VALUES (5004, 'Soda', 'Beverages', 1.75, NULL);
INSERT INTO dimproduct VALUES (5005, 'Headphones', 'Electronics', 49.99, NULL);
INSERT INTO dimproduct VALUES (5006, 'Detergent', 'Household', 8.99, NULL);


-- Table: dimsupplier
DROP TABLE IF EXISTS dimsupplier CASCADE;
CREATE TABLE dimsupplier (
    supplierkey INTEGER PRIMARY KEY,
    suppliername VARCHAR(255),
    contactinfo VARCHAR(255)
);

-- Insert data into dimsupplier
INSERT INTO dimsupplier VALUES (2001, 'Acme Foods', NULL);
INSERT INTO dimsupplier VALUES (2002, 'Fresh Farm', NULL);
INSERT INTO dimsupplier VALUES (2003, 'TechSource', NULL);


-- Table: dimcustomer
DROP TABLE IF EXISTS dimcustomer CASCADE;
CREATE TABLE dimcustomer (
    customerkey INTEGER PRIMARY KEY,
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    email VARCHAR(255),
    segment VARCHAR(255),
    city VARCHAR(255),
    validfrom VARCHAR(255),
    validto VARCHAR(255)
);

-- Insert data into dimcustomer
INSERT INTO dimcustomer VALUES (1001, 'Ava', 'Ng', 'ava@example.com', 'Regular', NULL, 2025-10-08, '9999-12-31');
INSERT INTO dimcustomer VALUES (1002, 'Liam', 'Chen', 'liam@example.com', 'Regular', NULL, 2025-10-08, '9999-12-31');
INSERT INTO dimcustomer VALUES (1003, 'Sofia', 'Rossi', 'sofia@example.com', 'Regular', NULL, 2025-10-08, '9999-12-31');
INSERT INTO dimcustomer VALUES (1004, 'Marco', 'Dubois', 'marco@example.com', 'Regular', NULL, 2025-10-08, '9999-12-31');


-- Table: dimpayment
DROP TABLE IF EXISTS dimpayment CASCADE;
CREATE TABLE dimpayment (
    paymentkey INTEGER PRIMARY KEY,
    paymenttype VARCHAR(255)
);

-- Insert data into dimpayment
INSERT INTO dimpayment VALUES (8001, 'Card');
INSERT INTO dimpayment VALUES (8002, 'Cash');
INSERT INTO dimpayment VALUES (8003, 'Card');
INSERT INTO dimpayment VALUES (8004, 'Card');
INSERT INTO dimpayment VALUES (8005, 'Voucher');


-- Table: factsales
DROP TABLE IF EXISTS factsales CASCADE;

                    CREATE TABLE factsales (
                        saleid INTEGER,
                        datekey INTEGER,
                        storeid INTEGER,
                        productid INTEGER,
                        customerid INTEGER,
                        paymentkey INTEGER,
                        quantity INTEGER,
                        salesamount DECIMAL(18,2),
                        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
                        FOREIGN KEY (storeid) REFERENCES dimstore(storekey),
                        FOREIGN KEY (productid) REFERENCES dimproduct(productkey),
                        FOREIGN KEY (customerid) REFERENCES dimcustomer(customerkey),
                        FOREIGN KEY (paymentkey) REFERENCES dimpayment(paymentkey)
                    );
                    
-- Insert data into factsales
INSERT INTO factsales VALUES (7001.0, 1.0, 3001.0, 5001.0, 1001.0, 8001.0, 2.0, 13.0);
INSERT INTO factsales VALUES (7001.0, 1.0, 3001.0, 5003.0, 1001.0, 8001.0, 3.0, 7.5);
INSERT INTO factsales VALUES (7002.0, 2.0, 3002.0, 5005.0, 1002.0, NULL, 1.0, 49.99);
INSERT INTO factsales VALUES (7002.0, 2.0, 3002.0, 5004.0, 1002.0, NULL, 2.0, 3.5);
INSERT INTO factsales VALUES (7003.0, 3.0, 3001.0, 5002.0, 1001.0, 8002.0, 1.0, 4.0);
INSERT INTO factsales VALUES (7003.0, 3.0, 3001.0, 5006.0, 1001.0, 8002.0, 2.0, 17.98);
INSERT INTO factsales VALUES (7004.0, 4.0, 3001.0, 5001.0, 1003.0, 8003.0, 1.0, 6.5);
INSERT INTO factsales VALUES (7004.0, 4.0, 3001.0, 5004.0, 1003.0, 8003.0, 4.0, 7.0);
INSERT INTO factsales VALUES (7005.0, 5.0, 3002.0, 5005.0, 1004.0, 8004.0, 2.0, 99.98);
INSERT INTO factsales VALUES (7006.0, 6.0, 3001.0, 5003.0, 1002.0, 8005.0, 5.0, 12.5);
INSERT INTO factsales VALUES (7006.0, 6.0, 3001.0, 5006.0, 1002.0, 8005.0, 1.0, 8.99);
INSERT INTO factsales VALUES (7007.0, 7.0, 3002.0, 5002.0, 1003.0, NULL, 3.0, 12.0);
INSERT INTO factsales VALUES (7007.0, 7.0, 3002.0, 5004.0, 1003.0, NULL, 1.0, 1.75);


COMMIT;
