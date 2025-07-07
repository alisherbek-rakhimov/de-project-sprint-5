from airflow.providers.http.hooks.http import HttpHook
from lib.pg_connect import ConnectionBuilder
import json

def load_deliveries_from_api(**context):
    http_hook = HttpHook(method='GET', http_conn_id='my_api_connection')
    response = http_hook.run(endpoint='/deliveries')
    deliveries_data = response.json()

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            for delivery in deliveries_data:
                # Find courier's internal id by _id
                cur.execute(
                    "SELECT id FROM stg.couriers WHERE _id = %s;",
                    (delivery["courier_id"],)
                )
                courier_row = cur.fetchone()
                if not courier_row:
                    # Optionally, skip or raise error if courier not found
                    continue
                courier_db_id = courier_row[0]
                cur.execute(
                    """
                    INSERT INTO stg.deliveries (delivery_id, data)
                    VALUES (%s, %s)
                    ON CONFLICT (delivery_id) DO UPDATE SET data = EXCLUDED.data;
                    """,
                    (delivery["delivery_id"], json.dumps(delivery, ensure_ascii=False))
                )
    print(deliveries_data)
    return deliveries_data

def load_deliveries_stg_to_dds(**context):
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT delivery_id, data FROM stg.deliveries;")
            for delivery_id, data in cur.fetchall():
                if isinstance(data, dict):
                    delivery = data
                else:
                    delivery = json.loads(data)
                courier_ext_id = delivery["courier_id"]
                cur.execute("SELECT id FROM dds.dm_couriers WHERE _id = %s;", (courier_ext_id,))
                courier_row = cur.fetchone()
                if not courier_row:
                    continue
                courier_db_id = courier_row[0]
                cur.execute(
                    """
                    INSERT INTO dds.dm_deliveries (
                        order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                    """,
                    (
                        delivery["order_id"],
                        delivery["order_ts"],
                        delivery["delivery_id"],
                        courier_db_id,
                        delivery["address"],
                        delivery["delivery_ts"],
                        delivery["rate"],
                        delivery["sum"],
                        delivery["tip_sum"]
                    )
                )
    return True