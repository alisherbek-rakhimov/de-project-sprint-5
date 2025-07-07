from datetime import datetime
from logging import Logger

from lib import PgConnect
from lib.dict_util import json2str
from psycopg.rows import dict_row


class FctProductSalesLoader:
    WF_KEY = "dds_fct_product_sales_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------
    def load(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Get or initialize workflow settings
                cur.execute("""
                    SELECT workflow_settings
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = %s
                """, (self.WF_KEY,))
                wf_settings = cur.fetchone()

                if wf_settings:
                    last_loaded_ts = wf_settings['workflow_settings']['last_loaded_ts']
                else:
                    last_loaded_ts = '1900-01-01 00:00:00'
                    cur.execute("""
                        INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                        VALUES (%s, %s)
                    """, (self.WF_KEY, json2str({"last_loaded_ts": last_loaded_ts})))
                    self.log.info(f"Initialized workflow setting with timestamp {last_loaded_ts}")

                # Load product sales incrementally
                cur.execute("""
                    WITH new_orders AS (
                        -- Get orders that were updated since last load
                        SELECT 
                            o.object_id as order_key,
                            o.object_value::json as order_data,
                            o.update_ts
                        FROM stg.ordersystem_orders o
                        WHERE o.update_ts > %s
                            AND (o.object_value::json->>'final_status') = 'CLOSED'
                    ),
                    order_products AS (
                        -- Extract product information from order JSON
                        SELECT 
                            no.order_key,
                            (item->>'id') as product_id_str,
                            (item->>'name') as product_name,
                            (item->>'price')::NUMERIC as price,
                            (item->>'quantity')::INTEGER as quantity,
                            (item->>'price')::NUMERIC * (item->>'quantity')::INTEGER as total_sum,
                            no.update_ts
                        FROM new_orders no,
                             JSON_ARRAY_ELEMENTS(no.order_data->'order_items') as item
                    ),
                    bonus_data AS (
                        -- Extract bonus information from bonus events
                        SELECT 
                            (be.event_value::json->>'order_id') as order_id,
                            (product->>'product_id') as product_id_str,
                            (product->>'bonus_payment')::NUMERIC as bonus_payment,
                            (product->>'bonus_grant')::NUMERIC as bonus_grant
                        FROM stg.bonussystem_events be,
                             JSON_ARRAY_ELEMENTS((be.event_value::json)->'product_payments') as product
                        WHERE be.event_type = 'bonus_transaction'
                    ),
                    combined_data AS (
                        SELECT 
                            op.order_key,
                            op.product_id_str,
                            op.product_name,
                            op.price,
                            op.quantity,
                            op.total_sum,
                            COALESCE(bd.bonus_payment, 0) as bonus_payment,
                            COALESCE(bd.bonus_grant, 0) as bonus_grant
                        FROM order_products op
                        LEFT JOIN bonus_data bd
                            ON op.order_key = bd.order_id 
                            AND op.product_id_str = bd.product_id_str
                    ),
                    -- Get the correct product version that was active at the order time
                    product_matches AS (
                        SELECT DISTINCT ON (cd.order_key, cd.product_id_str)
                            cd.*,
                            dmo.id as order_id,
                            dp.id as product_id,
                            ts.ts as order_timestamp
                        FROM combined_data cd
                        INNER JOIN dds.dm_orders dmo ON cd.order_key = dmo.order_key
                        INNER JOIN dds.dm_timestamps ts ON dmo.timestamp_id = ts.id
                        INNER JOIN dds.dm_products dp ON cd.product_id_str = dp.product_id
                            AND cd.product_name = dp.product_name
                            AND cd.price = dp.product_price
                            AND ts.ts >= dp.active_from 
                            AND ts.ts < dp.active_to
                        ORDER BY cd.order_key, cd.product_id_str, dp.active_from DESC
                    )
                    INSERT INTO dds.fct_product_sales (
                        product_id,
                        order_id,
                        count,
                        price,
                        total_sum,
                        bonus_payment,
                        bonus_grant
                    )
                    SELECT 
                        pm.product_id,
                        pm.order_id,
                        pm.quantity as count,
                        pm.price,
                        pm.total_sum,
                        pm.bonus_payment,
                        pm.bonus_grant
                    FROM product_matches pm
                    -- Avoid duplicates by checking if this order-product combination already exists
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM dds.fct_product_sales fps
                        WHERE fps.order_id = pm.order_id 
                            AND fps.product_id = pm.product_id
                    )
                """, (last_loaded_ts,))

                rows_inserted = cur.rowcount
                
                # Update workflow checkpoint
                cur.execute("""
                    SELECT MAX(update_ts) as max_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %s
                """, (last_loaded_ts,))
                result = cur.fetchone()
                
                if result and result['max_ts']:
                    new_ts = result['max_ts']
                    cur.execute("""
                        UPDATE dds.srv_wf_settings
                        SET workflow_settings = %s
                        WHERE workflow_key = %s
                    """, (json2str({"last_loaded_ts": new_ts.strftime("%Y-%m-%d %H:%M:%S")}), self.WF_KEY))
                    self.log.info(f"Loaded {rows_inserted} product sales records. Checkpoint updated to {new_ts}.")
                else:
                    self.log.info("No new orders to process for product sales.")
