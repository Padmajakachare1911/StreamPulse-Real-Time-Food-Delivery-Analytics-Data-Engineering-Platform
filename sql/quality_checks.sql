-- Quality checks on what is stored in PostgreSQL. Every "problem_rows" value should be 0.
-- Run: docker exec -i streampulse-postgres psql -U streampulse -d streampulse -f - < sql/quality_checks.sql

SELECT 'rows in core.orders'                    AS check_name, count(*)                      AS value FROM core.orders
UNION ALL SELECT 'distinct record_uid',                 count(DISTINCT record_uid)           FROM core.orders
UNION ALL SELECT 'distinct order_id',                   count(DISTINCT order_id)             FROM core.orders
UNION ALL SELECT 'distinct business_hash',              count(DISTINCT business_hash)        FROM core.orders;

SELECT 'duplicate order_id (problem_rows)' AS check_name, count(*) AS problem_rows
FROM (SELECT order_id FROM core.orders GROUP BY order_id HAVING count(*) > 1) d
UNION ALL SELECT 'delivered without delivery date', count(*) FROM core.orders WHERE order_status = 'delivered' AND delivered_customer_ts IS NULL
UNION ALL SELECT 'delivery date but status not delivered', count(*) FROM core.orders WHERE order_status <> 'delivered' AND delivered_customer_ts IS NOT NULL
UNION ALL SELECT 'delivered before purchase', count(*) FROM core.orders WHERE delivered_customer_ts < purchase_ts
UNION ALL SELECT 'negative amounts', count(*) FROM core.orders WHERE items_price_total < 0 OR items_freight_total < 0 OR payments_value_total < 0
UNION ALL SELECT 'status outside the allowed set', count(*) FROM core.orders
          WHERE order_status NOT IN ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved');

-- Information (not problems): how data was handled.
SELECT 'dead-letter rows by stage/reason' AS info, stage, reason_code, count(*) AS rows
FROM core.dead_letter_records GROUP BY stage, reason_code ORDER BY stage, reason_code;

SELECT 'duplicates logged by type' AS info, duplicate_type, count(*) AS rows
FROM core.duplicate_records GROUP BY duplicate_type;

SELECT 'orders without items (kept, count 0)' AS info, count(*) AS rows FROM core.orders WHERE items_count = 0
UNION ALL SELECT 'orders without payments (kept, count 0)', count(*) FROM core.orders WHERE payments_count = 0;
