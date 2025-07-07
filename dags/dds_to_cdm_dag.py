import logging
import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/59 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'cdm', 'settlement'],
    is_paused_upon_creation=True
)
def dds_to_cdm_dag():
    # Create connection to the data warehouse
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="fill_dm_settlement_report")
    def fill_dm_settlement_report():
        """
        Fill the CDM settlement report table with aggregated data from DDS layer.
        This task calculates daily settlement reports for each restaurant including:
        - Order counts and totals
        - Bonus payments and grants
        - Processing fees (25% of total)
        - Restaurant rewards (total - fees - bonus payments)
        """
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                log.info("Starting to fill dm_settlement_report table")
                
                cur.execute("""
                    INSERT INTO cdm.dm_settlement_report (restaurant_id,
                                                          settlement_date,
                                                          restaurant_name,
                                                          orders_count,
                                                          orders_total_sum,
                                                          orders_bonus_payment_sum,
                                                          orders_bonus_granted_sum,
                                                          order_processing_fee,
                                                          restaurant_reward_sum)
                    SELECT r.id                                                                      as restaurant_id,
                           t.date                                                                    as settlement_date,
                           r.restaurant_name,
                           COUNT(DISTINCT o.id)                                                      as orders_count,
                           SUM(fps.total_sum)                                                        as orders_total_sum,
                           SUM(fps.bonus_payment)                                                    as orders_bonus_payment_sum,
                           SUM(fps.bonus_grant)                                                      as orders_bonus_granted_sum,

                           SUM(fps.total_sum) * 0.25                                                 as order_processing_fee,

                           SUM(fps.total_sum) - (SUM(fps.total_sum) * 0.25) - SUM(fps.bonus_payment) as restaurant_reward_sum

                    FROM dds.fct_product_sales fps
                             INNER JOIN dds.dm_orders o ON fps.order_id = o.id
                             INNER JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                             INNER JOIN dds.dm_timestamps t ON o.timestamp_id = t.id

                    WHERE o.order_status = 'CLOSED'

                    GROUP BY r.id,
                             r.restaurant_name,
                             t.date

                    ON CONFLICT (restaurant_id, settlement_date)
                        DO UPDATE SET restaurant_name          = EXCLUDED.restaurant_name,
                                      orders_count             = EXCLUDED.orders_count,
                                      orders_total_sum         = EXCLUDED.orders_total_sum,
                                      orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                                      orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                                      order_processing_fee     = EXCLUDED.order_processing_fee,
                                      restaurant_reward_sum    = EXCLUDED.restaurant_reward_sum
                """)
                
                rows_affected = cur.rowcount
                log.info(f"Settlement report updated/inserted {rows_affected} records")

    fill_dm_settlement_report()


dds_to_cdm_settlement_dag = dds_to_cdm_dag() 