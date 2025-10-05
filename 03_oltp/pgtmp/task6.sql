-- 1. Update a customer’s email (safely avoid unique-email conflicts)
-- UPDATE Customers
-- SET Email = $2
-- WHERE CustomerID = $1
--   AND NOT EXISTS (SELECT 1 FROM Customers WHERE Email = $2)
-- RETURNING CustomerID, FirstName, LastName, Email;
-- $1 = customer_id, $2 = new_email

-- 2. Move a purchase to a different store
UPDATE purchases p
SET p.storeid = $2
WHERE p.purchaseid = $1
RETURNING p.purchaseid, p.purchasedatetime, p.customerid, p.storeid

-- 3. Change payment method and amount for a purchase (idempotent on PurchaseID)
UPDATE payments p
SET p.paymentnethod = $2 AND AMOUNT = $3
WHERE p.purchaseid = $1

-- 4. Reassign a product to a category (create category if it doesn’t exist)
INSERT INTO categories c
VALUES $2
ON CONFLICT (c.categoryname) DO NOTHING;

UPDATE products pr
SET pr.categoryid = c.categoryid
FROM categories
WHERE pr.productid = $1 AND c.categoryname = $2;

-- 5. Optimistic price change (only if current price matches expected old price)
UPDATE products
SET price = $2
WHERE productid = $1
AND price = $3