-- Analytics views. Only KPIs that the real columns support (docs/07 section 6). Safe to re-run.
--
-- DEFINITIONS (also shown in the dashboard):
--   revenue          = payments_value_total  (what customers paid: items + freight, after vouchers)
--   order value      = payments_value_total of orders that have at least one payment row
--   delivery time    = delivered_customer_ts - purchase_ts, only when delivered_customer_ts exists
--   late delivery    = delivered_customer_ts > estimated_delivery_ts
--   USD values       = estimate: BRL value x ECB reference rate of the purchase day (Frankfurter API)

CREATE OR REPLACE VIEW analytics.orders_per_day AS
SELECT purchase_ts::date AS order_date,
       count(*)                          AS orders,
       sum(payments_value_total)         AS revenue_brl,
       sum(payments_value_usd)           AS revenue_usd_estimate
FROM core.orders GROUP BY 1;

CREATE OR REPLACE VIEW analytics.orders_per_month AS
SELECT date_trunc('month', purchase_ts)::date AS order_month,
       count(*)                          AS orders,
       sum(payments_value_total)         AS revenue_brl,
       sum(payments_value_usd)           AS revenue_usd_estimate
FROM core.orders GROUP BY 1;

CREATE OR REPLACE VIEW analytics.order_status_distribution AS
SELECT order_status, count(*) AS orders,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS percent_of_orders
FROM core.orders GROUP BY 1;

CREATE OR REPLACE VIEW analytics.revenue_summary AS
SELECT count(*)                                                        AS orders,
       sum(payments_value_total)                                       AS revenue_brl,
       sum(items_price_total)                                          AS items_value_brl,
       sum(items_freight_total)                                        AS freight_brl,
       round(avg(payments_value_total) FILTER (WHERE payments_count > 0), 2) AS avg_order_value_brl,
       sum(payments_value_usd)                                         AS revenue_usd_estimate,
       min(purchase_ts)                                                AS first_purchase,
       max(purchase_ts)                                                AS last_purchase
FROM core.orders;

-- An order can use several payment types, so these counts add up to more than the number of orders.
CREATE OR REPLACE VIEW analytics.payment_type_mix AS
SELECT t.payment_type, count(*) AS orders_using_type
FROM core.orders o, unnest(string_to_array(o.payment_types, ',')) AS t(payment_type)
WHERE o.payment_types IS NOT NULL
GROUP BY 1;

CREATE OR REPLACE VIEW analytics.delivery_time_by_month AS
SELECT date_trunc('month', purchase_ts)::date AS order_month,
       count(*) AS delivered_orders,
       round(avg(extract(epoch FROM (delivered_customer_ts - purchase_ts)) / 86400.0)::numeric, 2) AS avg_delivery_days,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (delivered_customer_ts - purchase_ts)) / 86400.0))::numeric, 2) AS median_delivery_days
FROM core.orders WHERE delivered_customer_ts IS NOT NULL GROUP BY 1;

CREATE OR REPLACE VIEW analytics.late_delivery_summary AS
SELECT count(*) AS delivered_orders,
       count(*) FILTER (WHERE delivered_customer_ts > estimated_delivery_ts) AS late_orders,
       round(100.0 * count(*) FILTER (WHERE delivered_customer_ts > estimated_delivery_ts) / NULLIF(count(*), 0), 2) AS late_percent,
       round(avg(extract(epoch FROM (delivered_customer_ts - purchase_ts)) / 86400.0)::numeric, 2) AS avg_delivery_days
FROM core.orders WHERE delivered_customer_ts IS NOT NULL;

CREATE OR REPLACE VIEW analytics.orders_by_state AS
SELECT customer_state, count(*) AS orders,
       sum(payments_value_total) AS revenue_brl,
       round(avg(payments_value_total) FILTER (WHERE payments_count > 0), 2) AS avg_order_value_brl
FROM core.orders GROUP BY 1;

-- Items of the orders that are in the warehouse, with the English category when a translation exists.
CREATE OR REPLACE VIEW analytics.top_product_categories AS
SELECT COALESCE(p.product_category_name_english, p.product_category_name, 'unknown') AS category,
       count(*) AS items_sold, sum(i.price) AS items_value_brl
FROM core.order_items i
JOIN core.orders o ON o.order_id = i.order_id
LEFT JOIN core.dim_products p ON p.product_id = i.product_id
GROUP BY 1;

CREATE OR REPLACE VIEW analytics.review_score_distribution AS
SELECT r.review_score, count(*) AS reviews
FROM core.order_reviews r JOIN core.orders o ON o.order_id = r.order_id
GROUP BY 1;

CREATE OR REPLACE VIEW analytics.api_enrichment_summary AS
SELECT count(*)                                              AS orders,
       count(*) FILTER (WHERE api_enriched)                  AS orders_enriched,
       round(100.0 * count(*) FILTER (WHERE api_enriched) / NULLIF(count(*), 0), 2) AS match_rate_percent,
       count(*) FILTER (WHERE fx_match_type = 'exact')                 AS matched_exact_date,
       count(*) FILTER (WHERE fx_match_type = 'previous_business_day') AS matched_previous_business_day,
       max(api_retrieved_at)                                 AS last_api_retrieval,
       max(api_http_status)                                  AS last_http_status
FROM core.orders;
