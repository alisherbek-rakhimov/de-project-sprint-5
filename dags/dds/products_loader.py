from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from psycopg.rows import dict_row
from datetime import datetime


class ProductLoader:
    WF_KEY = "stg_products_to_dds_workflow"
    FAR_FUTURE = datetime(2099, 12, 31, 0, 0, 0)
    DEFAULT_ACTIVE_FROM = datetime(2020, 1, 1, 0, 0, 0)  # Default historical date for active_from

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    def load_products(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:

                # Инициализация метки загрузки
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
                    self.log.info(f"Initialized workflow setting at {last_loaded_ts}")

                # Загружаем продукты одним SQL запросом с SCD2 логикой
                cur.execute("""
                    WITH new_restaurants AS (
                        SELECT 
                            object_value::json AS obj,
                            update_ts
                        FROM stg.ordersystem_restaurants
                        WHERE update_ts > %s
                    ),
                    restaurant_products AS (
                        SELECT 
                            obj->>'_id' AS restaurant_id,
                            update_ts,
                            jsonb_array_elements((obj->'menu')::jsonb) AS product
                        FROM new_restaurants
                    ),
                    parsed_products AS (
                        SELECT 
                            restaurant_id,
                            product->>'_id' AS product_id,
                            product->>'name' AS product_name,
                            (product->>'price')::numeric AS product_price,
                            update_ts
                        FROM restaurant_products
                    ),
                    restaurant_versions AS (
                        SELECT 
                            id AS restaurant_id_internal
                        FROM dds.dm_restaurants
                        WHERE active_to = %s
                    ),
                    changed_products AS (
                        SELECT 
                            pp.*,
                            rv.restaurant_id_internal,
                            CASE 
                                WHEN dp.product_name IS NULL OR dp.product_name != pp.product_name OR dp.product_price != pp.product_price
                                THEN true 
                                ELSE false 
                            END AS has_changes
                        FROM parsed_products pp
                        INNER JOIN restaurant_versions rv ON rv.restaurant_id_internal IN (
                            SELECT id FROM dds.dm_restaurants WHERE restaurant_id = pp.restaurant_id AND active_to = %s
                        )
                        LEFT JOIN dds.dm_products dp ON dp.product_id = pp.product_id 
                            AND dp.restaurant_id = rv.restaurant_id_internal 
                            AND dp.active_to = %s
                    )
                    INSERT INTO dds.dm_products (product_id, product_name, restaurant_id, product_price, active_from, active_to)
                    SELECT 
                        product_id, 
                        product_name, 
                        restaurant_id_internal, 
                        product_price, 
                        %s,  -- Use DEFAULT_ACTIVE_FROM instead of update_ts
                        %s
                    FROM changed_products
                    WHERE has_changes = true
                    ON CONFLICT (product_id, active_from) DO NOTHING
                """, (last_loaded_ts, self.FAR_FUTURE, self.FAR_FUTURE, self.FAR_FUTURE, self.DEFAULT_ACTIVE_FROM, self.FAR_FUTURE))

                # Note: We're using a simplified approach without closing old product versions
                # In a full SCD2 implementation, you would close old versions when products change
                # For now, we keep all products active with FAR_FUTURE as active_to

                # Обновляем контрольную точку
                cur.execute("""
                    SELECT COUNT(*) as processed_count, MAX(update_ts) as max_ts
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %s
                """, (last_loaded_ts,))
                result = cur.fetchone()
                
                if result and result['processed_count'] > 0:
                    new_ts = result['max_ts']
                    cur.execute("""
                        UPDATE dds.srv_wf_settings
                        SET workflow_settings = %s
                        WHERE workflow_key = %s
                    """, (json2str({"last_loaded_ts": new_ts.strftime("%Y-%m-%d %H:%M:%S")}), self.WF_KEY))
                    self.log.info(f"{result['processed_count']} restaurants processed for products. Checkpoint updated to {new_ts}.")
                else:
                    self.log.info("No new restaurants to process for products.")
