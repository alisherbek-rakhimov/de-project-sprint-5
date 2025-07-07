from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from psycopg.rows import dict_row
from datetime import datetime


class RestaurantLoader:
    WF_KEY = "stg_restaurants_to_dds_workflow"
    FAR_FUTURE_DATE = datetime(2099, 12, 31, 0, 0, 0)
    DEFAULT_ACTIVE_FROM = datetime(2020, 1, 1, 0, 0, 0)  # Default historical date for active_from

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    def load_restaurants(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # 1. Получаем или создаём настройки
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

                # 2. Process new/changed records by update_ts (simple upsert, no SCD2)
                cur.execute("""
                    SELECT object_value::json AS obj, update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %s
                    ORDER BY update_ts
                """, (last_loaded_ts,))
                rows = cur.fetchall()
                if not rows:
                    self.log.info("No new restaurants to process.")
                    return

                for row in rows:
                    obj = row["obj"]
                    update_ts = row["update_ts"]
                    restaurant_id = obj["_id"]
                    restaurant_name = obj["name"]

                    # Simple upsert using ON CONFLICT
                    # Use DEFAULT_ACTIVE_FROM instead of update_ts to ensure historical orders can be linked
                    cur.execute("""
                        INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (restaurant_id) 
                        DO UPDATE SET 
                            restaurant_name = EXCLUDED.restaurant_name
                            -- Don't update active_from on conflict to preserve historical linkage
                    """, (restaurant_id, restaurant_name, self.DEFAULT_ACTIVE_FROM, self.FAR_FUTURE_DATE))

                # 3. Update checkpoint
                new_ts = max(row["update_ts"] for row in rows)
                cur.execute("""
                    UPDATE dds.srv_wf_settings
                    SET workflow_settings = %s
                    WHERE workflow_key = %s
                """, (json2str({"last_loaded_ts": new_ts.strftime("%Y-%m-%d %H:%M:%S")}), self.WF_KEY))
                self.log.info(f"dm_restaurants loaded {len(rows)} restaurants up to {new_ts}.")