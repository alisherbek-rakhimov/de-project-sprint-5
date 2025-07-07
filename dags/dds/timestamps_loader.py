from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from psycopg.rows import dict_row
from datetime import datetime


class TimestampsLoader:
    WF_KEY = "stg_timestamps_to_dds_workflow"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    def load_timestamps(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Получаем last_loaded_ts
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

                # Загружаем уникальные временные метки одним SQL запросом
                cur.execute("""
                    WITH new_orders AS (
                        SELECT 
                            object_value::json AS obj,
                            update_ts
                        FROM stg.ordersystem_orders
                        WHERE update_ts > %s
                    ),
                    parsed_timestamps AS (
                        SELECT DISTINCT
                            obj->>'date' AS ts_str
                        FROM new_orders
                        WHERE obj->>'date' ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
                    ),
                    valid_timestamps AS (
                        SELECT 
                            ts_str::timestamp AS ts
                        FROM parsed_timestamps
                    )
                    INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                    SELECT
                        ts,
                        EXTRACT(YEAR FROM ts)::smallint,
                        EXTRACT(MONTH FROM ts)::smallint,
                        EXTRACT(DAY FROM ts)::smallint,
                        ts::date,
                        ts::time
                    FROM valid_timestamps
                    ON CONFLICT (ts) DO NOTHING
                """, (last_loaded_ts,))

                # Получаем количество обработанных записей и обновляем чекпоинт
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
                    """, (json2str({"last_loaded_ts": new_ts.strftime("%Y-%m-%d %H:%M:%S")}), self.WF_KEY))
                    self.log.info(f"{result['processed_count']} orders processed for timestamps. Checkpoint updated to {new_ts}.")
                else:
                    self.log.info("No new timestamps to process.")
