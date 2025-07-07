from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from psycopg.rows import dict_row


class UserLoader:
    WF_KEY = "stg_users_to_dds_workflow"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.log = log

    def load_users(self) -> None:
        with self.pg.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # 1. Получаем или создаём workflow_settings
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

                # 2. Загружаем пользователей одним SQL запросом
                cur.execute("""
                    WITH new_users AS (
                        SELECT 
                            object_value::json AS obj,
                            update_ts
                        FROM stg.ordersystem_users
                        WHERE update_ts > %s
                    ),
                    parsed_users AS (
                        SELECT 
                            obj->>'_id' AS user_id,
                            obj->>'name' AS user_name,
                            obj->>'login' AS user_login,
                            update_ts
                        FROM new_users
                    )
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    SELECT user_id, user_name, user_login
                    FROM parsed_users
                    ON CONFLICT (user_id) DO UPDATE SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login
                """, (last_loaded_ts,))

                # 3. Получаем количество обработанных записей и обновляем чекпоинт
                cur.execute("""
                    SELECT COUNT(*) as processed_count, MAX(update_ts) as max_ts
                    FROM stg.ordersystem_users
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
                    self.log.info(f"{result['processed_count']} users processed. Checkpoint updated to {new_ts}.")
                else:
                    self.log.info("No new users to load.")
