import logging
import pendulum
from airflow.decorators import dag, task

from dds.product_sales_loader import FctProductSalesLoader
from dds.orders_loaders import OrderLoader
from dds.products_loader import ProductLoader
from dds.timestamps_loader import TimestampsLoader
from dds.restaurants_loader import RestaurantLoader
from dds.users_loader import UserLoader  # Предполагается, что ваш UserLoader перенесён в dds
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    # schedule_interval='@once',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'dds', 'users'],
    is_paused_upon_creation=True
)
def sprint5_stg_to_dds_dag():
    # Создаем подключения к источнику и хранилищу.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_users")
    def load_users():
        loader = UserLoader(dwh_pg_connect, log)
        loader.load_users()

    @task(task_id="load_restaurants")
    def load_restaurants():
        loader = RestaurantLoader(dwh_pg_connect, log)
        loader.load_restaurants()

    @task(task_id="load_timestamps")
    def load_timestamps():
        loader = TimestampsLoader(dwh_pg_connect, log)
        loader.load_timestamps()

    @task(task_id="load_products")
    def load_products():
        loader = ProductLoader(dwh_pg_connect, log)
        loader.load_products()

    @task(task_id="load_orders")
    def load_orders():
        loader = OrderLoader(dwh_pg_connect, log)
        loader.load_orders()

    @task(task_id="load_product_sales")
    def load_product_sales():
        loader = FctProductSalesLoader(dwh_pg_connect, log)
        loader.load()

    load_users() >> load_restaurants() >> load_timestamps() >> load_products() >> load_orders() >> load_product_sales()


stg_to_dds_users_dag = sprint5_stg_to_dds_dag()
