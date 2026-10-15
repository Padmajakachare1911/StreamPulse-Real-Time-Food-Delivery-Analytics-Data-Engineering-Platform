-- Sample queries over the analytics views (also used for the demos and as evidence).
-- Run: docker cp sql/analytics_samples.sql streampulse-postgres:/tmp/a.sql ; docker exec streampulse-postgres psql -U streampulse -d streampulse -f /tmp/a.sql

\echo '--- revenue_summary (revenue = payments_value_total; USD is an ECB-rate estimate) ---'
SELECT * FROM analytics.revenue_summary;

\echo '--- order_status_distribution ---'
SELECT * FROM analytics.order_status_distribution ORDER BY orders DESC;

\echo '--- orders_per_month (first 6 and last 3 months) ---'
(SELECT * FROM analytics.orders_per_month ORDER BY order_month LIMIT 6)
UNION ALL
(SELECT * FROM analytics.orders_per_month ORDER BY order_month DESC LIMIT 3)
ORDER BY order_month;

\echo '--- payment_type_mix (an order can use several types) ---'
SELECT * FROM analytics.payment_type_mix ORDER BY orders_using_type DESC;

\echo '--- late_delivery_summary ---'
SELECT * FROM analytics.late_delivery_summary;

\echo '--- delivery_time_by_month (last 4 months) ---'
SELECT * FROM analytics.delivery_time_by_month ORDER BY order_month DESC LIMIT 4;

\echo '--- orders_by_state (top 5 by orders) ---'
SELECT * FROM analytics.orders_by_state ORDER BY orders DESC LIMIT 5;

\echo '--- top_product_categories (top 5 by items sold) ---'
SELECT * FROM analytics.top_product_categories ORDER BY items_sold DESC LIMIT 5;

\echo '--- review_score_distribution ---'
SELECT * FROM analytics.review_score_distribution ORDER BY review_score;

\echo '--- api_enrichment_summary ---'
SELECT * FROM analytics.api_enrichment_summary;
