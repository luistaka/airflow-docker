import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - datetime.timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
START_DATE = now_to_the_hour
DAG_NAME = "read_postgres"

dag = DAG(
    DAG_NAME,
    schedule=None,
    default_args={"depends_on_past": False},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)