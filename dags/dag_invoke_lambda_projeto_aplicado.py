import json
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['no-reply@youse.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

with DAG(
    "dag_invoke_lambda_projeto_aplicado",
    description='Run built-in python app on Amazon Lambda',
    schedule=Variable.get('CRON_START_LAMBDA_PROJ_APLICADO', None),
    default_args=default_args,
    # dagrun_timeout=timedelta(hours=2),
    start_date=datetime.today() - timedelta(days=int(Variable.get('START_DATE_DAYS_AGO', 2))),
    catchup=False,
    tags=['INGESTOR', 'LAMBDA', 'PROJETO_APLICADO'],
) as dag:
    
    run_start = EmptyOperator(task_id="Start")

    api_function = Variable.get("api_function", None)
    api_key = Variable.get("api_key", None)
    EVENT_1 = json.dumps({"symbol": "MXRF11.SAO", "function": api_function, "apikey": api_key})
    LAMBDA_FUNCTION_NAME_1 = "lambdas-production-ApiStockFunction-r27CdJJ28E99"
    

    sheet_id = '1Cqpt01ciDVWYXhOH6VhWvL1G4HYtx4Z_crxcDnIDzx8'
    sheet_name = 'RENDA_VARIAVEL'
    EVENT_2 = json.dumps({"SHEET_ID": sheet_id, "SHEET_NAME": sheet_name})
    LAMBDA_FUNCTION_NAME_2 = "lambdas-production-GsheetFunction-KwAqjFCDXraF"

    # Start task group definition
    with TaskGroup(group_id='group1') as tg1:

        lambda_1 = AwsLambdaInvokeFunctionOperator(
            task_id='invoke_lambda_function_api_stock',
            function_name=LAMBDA_FUNCTION_NAME_1,
            invocation_type="Event",
            payload=EVENT_1,
            aws_conn_id="aws_conn_id"
        )

        lambda_2 = AwsLambdaInvokeFunctionOperator(
            task_id='invoke_lambda_function_gsheet',
            function_name=LAMBDA_FUNCTION_NAME_2,
            invocation_type="Event",
            payload=EVENT_2,
            aws_conn_id="aws_conn_id"
        )

        [lambda_1, lambda_2]

    run_end = EmptyOperator(task_id="End")

    run_start >> tg1 >> run_end



