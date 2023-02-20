import json
import pendulum
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator

DAG_NAME = "dag_invoke_lambda"
with DAG(
    DAG_NAME,
    schedule="*/30 * * * *",
    default_args={"depends_on_past": False},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    
    run_start = EmptyOperator(task_id="Start")

    api_function = Variable.get("api_function")
    api_key = Variable.get("api_key")
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

    run_end = EmptyOperator(task_id="End", dag=dag)

    run_start >> tg1 >> run_end



