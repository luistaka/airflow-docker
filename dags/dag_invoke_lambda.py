
import json
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator


default_args = {
    'owner': 'youse',
    'depends_on_past': False,
    'email': ['no-reply@youse.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

with DAG(
    "dag_invoke_lambda_zendesk",
    description='Run built-in Zendesk Ingestion on Amazon Lambda',
    schedule=Variable.get('CRON_START_YOUSE_LAMBDA_ZENDESK', None),
    default_args=default_args,
    start_date=datetime.today() - timedelta(days=int(Variable.get('START_DATE_DAYS_AGO', 2))),
    catchup=False,
    tags=['1-DATALAKE', '2-INGESTOR', '3-LAMBDA'],
) as dag:
    
    run_start = EmptyOperator(task_id="Start")

    LAMBDA_FUNCTION_NAME_1 = Variable.get('LAMBDA_ZENDESK_TICKET', '')
    LAMBDA_FUNCTION_NAME_2 = Variable.get('LAMBDA_ZENDESK_CHAT', '')
    LAMBDA_FUNCTION_NAME_3 = Variable.get('LAMBDA_ZENDESK_USER', '')

    # Start task group definition
    with TaskGroup(group_id='pre-processing') as tg1:

        lambda_1 = AwsLambdaInvokeFunctionOperator(
            task_id='invoke_lambda_zendesk_ticket',
            function_name=LAMBDA_FUNCTION_NAME_1,
            aws_conn_id=Variable.get('aws_conn_id', None)
        )

        lambda_2 = AwsLambdaInvokeFunctionOperator(
            task_id='invoke_lambda_zendesk_chat',
            function_name=LAMBDA_FUNCTION_NAME_2,
            aws_conn_id=Variable.get('aws_conn_id', None)
        )

        lambda_3 = AwsLambdaInvokeFunctionOperator(
            task_id='invoke_lambda_zendesk_user',
            function_name=LAMBDA_FUNCTION_NAME_3,
            aws_conn_id=Variable.get('aws_conn_id', None)
        )

        [lambda_1, lambda_2, lambda_3]

    run_end = EmptyOperator(task_id="End")

    run_start >> tg1 >> run_end