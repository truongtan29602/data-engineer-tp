
-- 1. Unpaid purchases (no payment recorded)
-- SELECT p.PurchaseID, p.PurchaseDateTime, c.FirstName, c.LastName, s.StoreName
-- FROM Purchases p
-- JOIN Customers c ON c.CustomerID = p.CustomerID
-- JOIN Stores s ON s.StoreID = p.StoreID
-- LEFT JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
-- WHERE pay.PurchaseID IS NULL
-- ORDER BY p.PurchaseDateTime DESC;

-- 2. Customer’s last 5 purchases with item count and paid amount
-- SELECT customerid, purchaseid, purchasedatetime, item_count, total_amount
-- FROM (
--     SELECT 
--         p.customerid,
--         p.purchaseid,
--         p.purchasedatetime,
--         COUNT(pi.quantity) AS item_count,
--         SUM(pi.quantity * pr.price) AS total_amount,
--         ROW_NUMBER() OVER (PARTITION BY p.customerid ORDER BY p.purchasedatetime DESC) AS rn
--     FROM purchases p
--     JOIN purchase_items pi ON p.purchaseid = pi.purchaseid
-- 	   LEFT JOIN products pr ON pi.productid = pr.productid
--     GROUP BY p.customerid, p.purchaseid, p.purchasedatetime
-- ) sub
-- WHERE rn <= 5
-- ORDER BY customerid, purchaseid DESC;

-- 3. Revenue & transaction count per store in a date range
-- SELECT 
--     s.storeid,
--     s.storename,
--     COUNT(DISTINCT p.purchaseid) AS transaction_count,
--     SUM(pr.price * pi.quantity)   AS total_revenue
-- FROM stores s
-- JOIN purchases p ON s.storeid = p.storeid
-- JOIN purchase_items pi ON p.purchaseid = pi.purchaseid
-- LEFT JOIN products pr ON pi.productid = pr.productid
-- WHERE p.purchasedatetime BETWEEN '2000-01-01' AND '2025-09-30' -- date range
-- GROUP BY s.storeid, s.storename
-- ORDER BY total_revenue DESC;

-- 4. Top 5 products by revenue in the last N days
-- SELECT 
--     pr.productid,
--     pr.productname,
--     SUM(pr.price * pi.quantity) AS total_revenue
-- FROM purchase_items pi
-- JOIN purchases pu ON pi.purchaseid = pu.purchaseid
-- JOIN products pr ON pi.productid = pr.productid
-- WHERE pu.purchasedatetime >= CURRENT_DATE - INTERVAL '7 days'
-- GROUP BY pr.productid, pr.productname
-- ORDER BY total_revenue DESC
-- LIMIT 5;

-- 5. Suppliers for a given product (comma-separated)
-- SELECT 
--     p.productid,
--     p.productname,
--     STRING_AGG(s.suppliername, ', ' ORDER BY s.suppliername) AS suppliers
-- FROM products p
-- JOIN product_suppliers ps ON p.productid = ps.productid
-- JOIN suppliers s ON ps.supplierid = s.supplierid
-- WHERE p.productid = 5001
-- GROUP BY p.productid, p.productname;

-- 6. Monthly revenue by category and by store's location a. can you add the year-over-year growth
SELECT 
    DATE_TRUNC('month', pu.purchasedatetime)::date AS month,
    c.categoryname,
    s.location,
    SUM(pr.price * pi.quantity) AS revenue
FROM purchases pu
JOIN purchase_items pi ON pu.purchaseid = pi.purchaseid
JOIN products pr ON pi.productid = pr.productid
JOIN categories c ON pr.categoryid = c.categoryid
JOIN stores s ON pu.storeid = s.storeid
GROUP BY DATE_TRUNC('month', pu.purchasedatetime), c.categoryname, s.location
ORDER BY month, c.categoryname, s.location;

-- 7. Monthly customer retention (“customers who bought in month N AND month N−1”.)
WITH monthly_customers AS (
    SELECT 
        DATE_TRUNC('month', pu.purchasedatetime)::timestamp AS month_start,
        pu.customerid
    FROM purchases pu
    GROUP BY DATE_TRUNC('month', pu.purchasedatetime), pu.customerid
),

retention AS (
    SELECT 
        curr.month_start AS datetime,
        COUNT(DISTINCT curr.customerid) AS total_customers,
        COUNT(DISTINCT curr.customerid) FILTER (
            WHERE prev.customerid IS NOT NULL
        ) AS retained_customers
    FROM monthly_customers curr
    LEFT JOIN monthly_customers prev
        ON prev.customerid = curr.customerid
       AND prev.month_start = curr.month_start - INTERVAL '1 month'
    GROUP BY curr.month_start
)

SELECT 
    datetime,
    total_customers,
    retained_customers,
    ROUND(
        100.0 * retained_customers / NULLIF(total_customers, 0), 
        2
    ) AS retention_rate_pct
FROM retention
ORDER BY datetime;


