from airflow.providers.http.hooks.http import HttpHook
from lib.pg_connect import ConnectionBuilder
import json

def load_couriers_from_api(**context):
    http_hook = HttpHook(method='GET', http_conn_id='my_api_connection')
    response = http_hook.run(endpoint='/couriers')
    couriers_data = response.json()
    
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            for courier in couriers_data:
                cur.execute(
                    """
                    INSERT INTO stg.couriers (_id, data)
                    VALUES (%s, %s)
                    ON CONFLICT (_id) DO UPDATE SET data = EXCLUDED.data;
                    """,
                    (courier["_id"], json.dumps(courier, ensure_ascii=False))
                )

    # Process and save couriers_data as needed
    print(couriers_data)
    return couriers_data

def load_couriers_stg_to_dds(**context):
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT _id, data FROM stg.couriers;")
            for _id, data in cur.fetchall():
                if isinstance(data, dict):
                    courier = data
                else:
                    courier = json.loads(data)
                name = courier["name"]
                cur.execute(
                    """
                    INSERT INTO dds.dm_couriers (_id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (_id) DO UPDATE SET name = EXCLUDED.name;
                    """,
                    (_id, name)
                )
    return True