from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from psycopg.rows import dict_row
from datetime import datetime


class OrderLoader:
    WF_KEY = "stg_orders_to_dds_workflow"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    def load_orders(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:

                # 1. Получаем/инициализируем состояние workflow
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
                    settings_json = json2str({"last_loaded_ts": last_loaded_ts})
                    cur.execute("""
                        INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                        VALUES (%s, %s)
                    """, (self.WF_KEY, settings_json))
                    self.log.info(f"Initialized workflow setting at {last_loaded_ts}")

                # 2. Загружаем заказы одним SQL запросом
                cur.execute("""
                    WITH new_orders AS (
                        SELECT 
                            object_value::json AS obj,
                            update_ts
                        FROM stg.ordersystem_orders
                        WHERE update_ts > %s
                    ),
                    parsed_orders AS (
                        SELECT 
                            obj->>'_id' AS order_key,
                            obj->>'final_status' AS order_status,
                            obj->'user'->>'id' AS user_external_id,
                            obj->'restaurant'->>'id' AS restaurant_external_id,
                            obj->>'date' AS date_str,
                            update_ts
                        FROM new_orders
                    ),
                    valid_orders AS (
                        SELECT 
                            po.*,
                            u.id AS user_id,
                            r.id AS restaurant_id,
                            t.id AS timestamp_id
                        FROM parsed_orders po
                        INNER JOIN dds.dm_users u ON u.user_id = po.user_external_id
                        INNER JOIN dds.dm_restaurants r ON r.restaurant_id = po.restaurant_external_id
                            AND po.date_str::timestamp BETWEEN r.active_from AND r.active_to
                        INNER JOIN dds.dm_timestamps t ON t.ts = po.date_str::timestamp
                        WHERE po.date_str ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
                    )
                    INSERT INTO dds.dm_orders (order_key, order_status, user_id, restaurant_id, timestamp_id)
                    SELECT order_key, order_status, user_id, restaurant_id, timestamp_id
                    FROM valid_orders
                    ON CONFLICT (order_key) DO NOTHING
                """, (last_loaded_ts,))

                # 3. Получаем количество обработанных записей и обновляем чекпоинт
                cur.execute("""
                    SELECT COUNT(*) as processed_count, MAX(update_ts) as max_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %s
                """, (last_loaded_ts,))
                result = cur.fetchone()
                
                if result and result['processed_count'] > 0:
                    new_ts = result['max_ts']
                    cur.execute("""
                        UPDATE dds.srv_wf_settings
                        SET workflow_settings = %s
                        WHERE workflow_key = %s
                    """, (
                        json2str({"last_loaded_ts": new_ts.strftime("%Y-%m-%d %H:%M:%S")}),
                        self.WF_KEY
                    ))
                    self.log.info(f"{result['processed_count']} orders processed. Checkpoint updated to {new_ts}.")
                else:
                    self.log.info("No new orders to load.")
