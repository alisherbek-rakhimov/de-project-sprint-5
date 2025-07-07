import logging
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


def fill_dm_courier_ledger(**context):
    """Fill the CDM courier ledger table with aggregated data from DDS layer"""
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            log.info("Starting to fill dm_courier_ledger table")
            
            # Ensure CDM schema exists
            cur.execute("CREATE SCHEMA IF NOT EXISTS cdm;")
            
            cur.execute("""
                INSERT INTO cdm.dm_courier_ledger (
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_reward_sum
                )
                SELECT 
                    c.id as courier_id,
                    c.name as courier_name,
                    EXTRACT(YEAR FROM d.order_ts)::INT as settlement_year,
                    EXTRACT(MONTH FROM d.order_ts)::INT as settlement_month,
                    COUNT(DISTINCT d.order_id) as orders_count,
                    SUM(d.sum) as orders_total_sum,
                    AVG(d.rate) as rate_avg,
                    SUM(d.sum) * 0.25 as order_processing_fee,
                    SUM(d.sum) as courier_order_sum,
                    SUM(d.tip_sum) as courier_tips_sum,
                    CASE 
                        WHEN AVG(d.rate) < 4 THEN GREATEST(SUM(d.sum) * 0.05, 100)
                        WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN GREATEST(SUM(d.sum) * 0.07, 150)
                        WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN GREATEST(SUM(d.sum) * 0.08, 175)
                        WHEN AVG(d.rate) >= 4.9 THEN GREATEST(SUM(d.sum) * 0.10, 200)
                    END as courier_reward_sum
                FROM dds.dm_couriers c
                INNER JOIN dds.dm_deliveries d ON c.id = d.courier_id
                GROUP BY 
                    c.id,
                    c.name,
                    EXTRACT(YEAR FROM d.order_ts),
                    EXTRACT(MONTH FROM d.order_ts)
                ON CONFLICT (courier_id, settlement_year, settlement_month)
                    DO UPDATE SET 
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
            """)
            
            rows_affected = cur.rowcount
            log.info(f"Courier ledger updated/inserted {rows_affected} records")
    
    return True
