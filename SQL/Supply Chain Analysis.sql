/* =========================================================
   📦 SUPPLY CHAIN ANALYSIS – SQL (END-TO-END)
   ========================================================= */

/* =========================================================
   1. DEMAND ANALYSIS
   ========================================================= */

-- Business Question: Which months are peak demand months per product?
WITH qty_month AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        product_id,
        SUM(quantity) AS total_quantity
    FROM historical_orders
    GROUP BY month, product_id
)

SELECT month, product_id, total_quantity
FROM (
    SELECT *,
           DENSE_RANK() OVER(PARTITION BY product_id ORDER BY total_quantity DESC) AS rnk
    FROM qty_month
) ranked
WHERE rnk = 1;


-- Business Question: Which region demands the most per product?
WITH qty_region AS (
    SELECT 
        dm.region,
        ho.product_id,
        SUM(ho.quantity) AS total_quantity
    FROM historical_orders ho
    JOIN dealer_master dm USING(dealer_id)
    GROUP BY dm.region, ho.product_id
)

SELECT region, product_id, total_quantity
FROM (
    SELECT *,
           DENSE_RANK() OVER(PARTITION BY product_id ORDER BY total_quantity DESC) AS rnk
    FROM qty_region
) ranked
WHERE rnk = 1;


-- Business Question: Which product drives overall volume?
WITH qty_product AS (
    SELECT 
        product_id,
        SUM(quantity) AS total_quantity
    FROM historical_orders
    GROUP BY product_id
)

SELECT *,
       ROUND((total_quantity / SUM(total_quantity) OVER()) * 100, 2) AS volume_pct
FROM qty_product;



/* =========================================================
   2. LEAD TIME ANALYSIS
   ========================================================= */

-- Base table for lead time calculation
WITH base AS (
    SELECT 
        dm.region,
        pm.product_name,
        ho.order_date,
        s.ship_date,

        DATE_ADD(s.ship_date, INTERVAL pm.lead_time_days DAY) AS delivery_date,

        DATEDIFF(
            DATE_ADD(s.ship_date, INTERVAL pm.lead_time_days DAY),
            ho.order_date
        ) AS lead_time

    FROM shipments s
    JOIN historical_orders ho 
        ON s.dealer_id = ho.dealer_id 
        AND s.product_id = ho.product_id
    JOIN dealer_master dm 
        ON s.dealer_id = dm.dealer_id
    JOIN product_master pm 
        ON s.product_id = pm.product_id
)

-- Business Question: Which region is slowest?
SELECT 
    region,
    ROUND(AVG(lead_time), 2) AS avg_lead_time
FROM base
GROUP BY region
ORDER BY avg_lead_time DESC;


-- Business Question: Which product faces highest delay?
SELECT 
    product_name,
    ROUND(AVG(lead_time), 2) AS avg_lead_time
FROM base
GROUP BY product_name
ORDER BY avg_lead_time DESC;


-- Business Question: Is delay consistent or variable?
WITH base_variability AS (
    SELECT 
        s.shipment_id,
        dm.region,
        pm.product_name,

        DATEDIFF(
            DATE_ADD(s.ship_date, INTERVAL pm.lead_time_days DAY),
            ho.order_date
        ) AS lead_time,

        ROW_NUMBER() OVER (
            PARTITION BY s.shipment_id 
            ORDER BY ho.order_date DESC
        ) AS rn

    FROM shipments s
    JOIN dealer_master dm ON s.dealer_id = dm.dealer_id
    JOIN product_master pm ON s.product_id = pm.product_id
    JOIN historical_orders ho 
        ON s.dealer_id = ho.dealer_id 
        AND s.product_id = ho.product_id
        AND ho.order_date <= s.ship_date
)

SELECT 
    region,
    ROUND(AVG(lead_time), 2) AS avg_lead_time,
    ROUND(STDDEV(lead_time), 2) AS variability
FROM base_variability
WHERE rn = 1
GROUP BY region;



/* =========================================================
   3. DISTANCE & LOGISTICS ANALYSIS
   ========================================================= */

-- Business Question: Are long distances causing delays?
SELECT 
    region,
    ROUND(AVG(distance_from_plant), 2) AS avg_distance
FROM dealer_master
GROUP BY region
ORDER BY avg_distance DESC;


-- Business Question: Are long-distance shipments frequent?
WITH shipment_distance AS (
    SELECT 
        s.shipment_id,
        dm.region,
        dm.distance_from_plant
    FROM shipments s
    JOIN dealer_master dm ON s.dealer_id = dm.dealer_id
)

SELECT 
    region,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN distance_from_plant > 700 THEN 1 ELSE 0 END) AS long_distance_shipments,
    ROUND(
        (SUM(CASE WHEN distance_from_plant > 700 THEN 1 ELSE 0 END) / COUNT(*)) * 100,
        2
    ) AS long_distance_pct
FROM shipment_distance
GROUP BY region;



/* =========================================================
   4. COST ANALYSIS
   ========================================================= */

-- Business Question: Which region has highest cost per unit?
WITH region_cost AS (
    SELECT 
        dm.region,
        (s.freight_cost / s.quantity) AS cost_per_unit
    FROM shipments s
    JOIN dealer_master dm USING(dealer_id)
)

SELECT 
    region,
    ROUND(AVG(cost_per_unit), 2) AS avg_cost_unit
FROM region_cost
GROUP BY region
ORDER BY avg_cost_unit DESC;


-- Business Question: Is cost linked to distance?
WITH region_cost_km AS (
    SELECT 
        dm.region,
        (s.freight_cost / dm.distance_from_plant) AS cost_per_km
    FROM shipments s
    JOIN dealer_master dm USING(dealer_id)
)

SELECT 
    region,
    ROUND(AVG(cost_per_km), 2) AS avg_cost_km
FROM region_cost_km
GROUP BY region
ORDER BY avg_cost_km DESC;


-- Business Question: Distance vs Cost relationship
SELECT 
    dm.region,
    ROUND(AVG(dm.distance_from_plant), 2) AS avg_distance,
    ROUND(AVG(s.freight_cost / s.quantity), 2) AS avg_cost
FROM dealer_master dm
JOIN shipments s USING(dealer_id)
GROUP BY dm.region
ORDER BY avg_distance DESC;



/* =========================================================
   5. DEMAND vs INVENTORY ANALYSIS
   ========================================================= */

-- Business Question: Is inventory aligned with demand?
WITH demand AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        SUM(quantity) AS total_demand
    FROM historical_orders
    GROUP BY month
),

inventory AS (
    SELECT 
        DATE_FORMAT(date, '%Y-%m') AS month,
        SUM(stock_qty) AS total_inventory
    FROM inventory_snapshots
    GROUP BY month
)

SELECT 
    d.month,
    d.total_demand,
    i.total_inventory,
    ROUND(i.total_inventory / d.total_demand, 2) AS inventory_coverage_ratio
FROM demand d
JOIN inventory i ON d.month = i.month
ORDER BY d.month;


-- Business Question: Is there stockout risk?
WITH demand AS (
    SELECT 
        DATE_FORMAT(order_date, '%Y-%m') AS month,
        SUM(quantity) AS total_demand
    FROM historical_orders
    GROUP BY month
),

inventory AS (
    SELECT 
        DATE_FORMAT(date, '%Y-%m') AS month,
        SUM(stock_qty) AS total_inventory
    FROM inventory_snapshots
    GROUP BY month
)

SELECT 
    d.month,
    d.total_demand,
    i.total_inventory,
    ROUND(i.total_inventory / d.total_demand, 2) AS inventory_coverage_ratio,

    CASE 
        WHEN (i.total_inventory / d.total_demand) > 1 THEN 'Enough stock'
        WHEN (i.total_inventory / d.total_demand) = 1 THEN 'Balanced'
        ELSE 'Stockout Risk'
    END AS risk_segment

FROM demand d
JOIN inventory i ON d.month = i.month
ORDER BY d.month;