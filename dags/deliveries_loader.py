from airflow.providers.http.hooks.http import HttpHook
from lib.pg_connect import ConnectionBuilder
import json
from datetime import datetime, timedelta


def load_deliveries_from_api(**context):
    http_hook = HttpHook(method='GET', http_conn_id='my_api_connection')

    # Option 2: Use DAG execution context for date filtering (recommended)
    # This allows for incremental loading and easier backfilling
    execution_date = context.get('execution_date')
    if execution_date:
        # For scheduled runs, load data for the DAG's execution period
        end_date = execution_date
        start_date = end_date - timedelta(days=7)
    else:
        # Fallback to last 7 days if no execution context
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

    # Format dates according to API requirements
    from_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
    to_date = end_date.strftime('%Y-%m-%d %H:%M:%S')

    # Log the date range being processed
    print(f"Loading deliveries from {from_date} to {to_date}")

    # Initialize pagination variables
    offset = 0
    limit = 50  # API returns 50 records at a time
    all_deliveries = []

    # Loop to handle pagination
    while True:
        # Prepare parameters for the API request
        params = {
            'from': from_date,
            'to': to_date,
            'offset': offset,
            'limit': limit,
            'sort_field': 'date',
            'sort_direction': 'asc'
        }

        # Make API request with parameters
        response = http_hook.run(
            endpoint='/deliveries',
            data=params
        )

        deliveries_data = response.json()

        # If no data returned, break the loop
        if not deliveries_data:
            break

        # Add current batch to all deliveries
        all_deliveries.extend(deliveries_data)

        # If we got less than the limit, we've reached the end
        if len(deliveries_data) < limit:
            break

        # Increment offset for next iteration
        offset += limit

    # Process all deliveries
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            # Ensure idempotency: clean up deliveries for the date range before loading
            # This prevents duplicate data if the DAG is re-run for the same period
            cur.execute(
                """
                DELETE
                FROM stg.deliveries
                WHERE (data ->>'delivery_ts')::timestamp >= %s
                  AND (data ->>'delivery_ts'):: timestamp <= %s;
                """,
                (from_date, to_date)
            )

            for delivery in all_deliveries:
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
                    VALUES (%s, %s) ON CONFLICT (delivery_id) DO
                    UPDATE SET data = EXCLUDED.data;
                    """,
                    (delivery["delivery_id"], json.dumps(delivery, ensure_ascii=False))
                )

    print(f"Loaded {len(all_deliveries)} deliveries from {from_date} to {to_date}")
    return all_deliveries


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
                    INSERT INTO dds.dm_deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO
                    UPDATE SET
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