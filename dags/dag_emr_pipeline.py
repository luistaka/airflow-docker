import ast
import json
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrModifyClusterOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator, S3ListPrefixesOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators.emr.emr_execute_step_operator import EmrExecuteStepOperator
from operators.emr.emr_step_sensor_operator import EmrListStepSensorOperator

environment = Variable.get('environment', 'qa')

# -------------------------------------------------------------------------------------------
# Functions get checkpoint from dynamodb
# -------------------------------------------------------------------------------------------
def get_dynamodb_checkpoint(**kwargs):

    dynamodb = DynamoDBHook(aws_conn_id=Variable.get('aws_conn_id', None)).get_conn()
    table = dynamodb.Table(f"datalakehouse-checkpoints-{environment}")
    batch_keys = {
        table.name: {
             "Keys": [{"key_name":  "bronze"}]
        }
    }
    response = dynamodb.batch_get_item(RequestItems=batch_keys)
    kwargs["ti"].xcom_push(key='bronze_checkpoint', value=[i['bronze'] for i in response['Responses'][f"datalakehouse-checkpoints-{environment}"]][0])

# -------------------------------------------------------------------------------------------
# Functions get checkpoint from dynamodb
# -------------------------------------------------------------------------------------------
def return_processing_paths(**kwargs):
    hook = S3Hook(aws_conn_id=Variable.get('aws_conn_id', None))
    value = kwargs["ti"].xcom_pull(task_ids='pre-processing.get_dynamodb_checkpoint', key='bronze_checkpoint')
    start_date = datetime.strptime(value, "%Y-%m-%d %H")
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    hours_mod = divmod((end_date - start_date).total_seconds(), 3600)[0]
    prefixs = [f"rabbitMQ-events/{(start_date + timedelta(hours=x)).strftime('%Y/%m/%d/%H')}" for x in range(1, int(hours_mod))]
    paths_valids = [f"s3a://br-youse-data-lake-{environment}/{prefix}/*" for prefix in prefixs if hook.check_for_prefix(prefix, delimiter="/", bucket_name=f"br-youse-data-lake-{environment}")==True]
    kwargs["ti"].xcom_push(key='bronze_paths', value=str(paths_valids))

# -------------------------------------------------------------------------------------------
# Functions to start crawler
# -------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------------
# Setting DAG default arguments
# -------------------------------------------------------------------------------------------
default_args = {
    'owner': 'youse',
    'depends_on_past': False,
    'email': ['no-reply@youse.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

with DAG(
        "dag_invoke_emr",
        description='Run built-in Spark app on Amazon EMR',
        schedule=Variable.get('CRON_START_YOUSE_EMR_PIPELINE', None),
        default_args=default_args,
        render_template_as_native_obj=True,
        # dagrun_timeout=timedelta(hours=2),
        start_date=datetime.today() - timedelta(days=int(Variable.get('START_DATE_DAYS_AGO', 2))),
        catchup=False,
        tags=['1-DATALAKE', '2-PROCESSING', '3-EMR'],
) as dag:

# -------------------------------------------------------------------------------------------
# Starting Dag
# -------------------------------------------------------------------------------------------
    run_start = EmptyOperator(task_id="Start")

# -------------------------------------------------------------------------------------------
# Pre-processing layer (remove temp files and read checkpoit from dynamodb)
# -------------------------------------------------------------------------------------------
    with TaskGroup(group_id='pre-processing') as tg_pre_processing:

        remove_temp_files = S3DeleteObjectsOperator(
            task_id='remove_temp_files',
            bucket=f"br-youse-data-lake-{environment}",
            keys=None,
            prefix= "datalakehouse/bronze/temp/",
            aws_conn_id=Variable.get('aws_conn_id', None),
            verify=None
        )

        get_checkpoint = PythonOperator(
            task_id='get_dynamodb_checkpoint',
            python_callable=get_dynamodb_checkpoint,
        )

        get_processing_paths = PythonOperator(
            task_id='return_processing_paths',
            python_callable=return_processing_paths, 
        )

        [remove_temp_files, get_checkpoint >> get_processing_paths]

# -------------------------------------------------------------------------------------------
# Setting job_flow parameters
# -------------------------------------------------------------------------------------------
    JOB_FLOW_OVERRIDES = {
        'Name': 'EMR Youse Pipeline = Luis',
        'ReleaseLabel': Variable.get('EMR_RELEASE_LABEL', 'emr-6.10.0'),
        'AutoTerminationPolicy': { 
            'IdleTimeout': 1200
        },
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': Variable.get('EMR_MASTER_INSTANCE_MARKET', 'SPOT'),
                    'InstanceRole': 'MASTER',
                    'InstanceType': Variable.get('EMR_MASTER_INSTANCE_TYPE', 'r5.xlarge'),
                    'InstanceCount': int(Variable.get('EMR_MASTER_INSTANCE_COUNT', 1)),
                },
                {
                    'Name': 'Core nodes',
                    'Market': Variable.get('EMR_CORE_INSTANCE_MARKET', 'SPOT'),
                    'InstanceRole': 'CORE',
                    'InstanceType': Variable.get('EMR_CORE_INSTANCE_TYPE', 'r5.xlarge'),
                    'InstanceCount': int(Variable.get('EMR_CORE_INSTANCE_COUNT', 1)),
                },
                {
                    'Name': 'Task nodes',
                    'Market': Variable.get('EMR_TASK_INSTANCE_MARKET', 'SPOT'),
                    'InstanceRole': 'TASK',
                    'InstanceType': Variable.get('EMR_TASK_INSTANCE_TYPE', 'r5.xlarge'),
                    'InstanceCount': int(Variable.get('EMR_TASK_INSTANCE_COUNT', 5)),
                }
            ],
            # 'Ec2KeyName': Variable.get('EMR_EC2KEYNAME', 'key-00fd7513faf83effc'),
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': Variable.get('EMR_EC2_SUBNET_ID', 'subnet-923a31db')
        },
        'LogUri': Variable.get('EMR_LOG_URL', 's3://aws-logs-493456159661-us-east-1/elasticmapreduce/'),
        'VisibleToAllUsers': True,
        'JobFlowRole': Variable.get('EMR_JOB_FLOW_ROLE', 'EMR_EC2_DefaultRole'),
        'ServiceRole': Variable.get('EMR_SERVICE_ROLE', 'EMR_DefaultRole'),
        'StepConcurrencyLevel': 20,
        'Applications': [
            {'Name': 'Hadoop'},
            {'Name': 'Livy'},
            {'Name': 'Spark'},
            {'Name': 'Hive'},
            {'Name': 'JupyterEnterpriseGateway'},
            {'Name': 'JupyterHub'}
        ],
        'Configurations': [
            {
                'Classification': 'delta-defaults',
                'Properties': {
                    'delta.enabled': 'true'
                }
            },
        ],
        'ManagedScalingPolicy' :{
            'ComputeLimits': {
                'UnitType': 'Instances',
                'MinimumCapacityUnits': 3,
                'MaximumCapacityUnits': 20,
                'MaximumOnDemandCapacityUnits': 0,
                'MaximumCoreCapacityUnits': 2
            }
        },
        'AutoTerminationPolicy': {
            "IdleTimeout": int(Variable.get('EMR__AUTO__TERMINATION__CLUSTER', 2400))
        },
    }

# -------------------------------------------------------------------------------------------
# Creating job_flow (emr cluster)
# -------------------------------------------------------------------------------------------
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=Variable.get('aws_conn_id', None),
        emr_conn_id=None,
    )

    wait_emr_cluster = EmrJobFlowSensor(
        task_id='wait_emr_cluster',
        job_flow_id=cluster_creator.output,
        target_states='WAITING',
        failed_states=None,
        aws_conn_id=Variable.get('aws_conn_id', None),
    )

# -------------------------------------------------------------------------------------------
# Processing bronze layer (create emr cluster)
# -------------------------------------------------------------------------------------------
    with TaskGroup(group_id='processing-bronze') as tg_bronze:

        emr_process_bronze = EmrExecuteStepOperator(
            task_id='emr_process_bronze',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
            execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src_lft_teste/datalakehouse/bronze/bronze.py",
            paths="{{ task_instance.xcom_pull(task_ids='pre-processing.return_processing_paths', dag_id='dag_invoke_emr', key='bronze_paths') }}",
            environment=environment,
            step_args=[
                '--AWS_VALIDATES=True',
                "--TEMP=True"
            ],
            step_name="emr_process_bronze",
            aws_conn_id=Variable.get('aws_conn_id', None)
        )

        step_checker = EmrListStepSensorOperator(
            task_id='watch_step_bronze',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
            list_step_id="{{ task_instance.xcom_pull(task_ids='processing-bronze.emr_process_bronze', key='step_id') }}",
            aws_conn_id=Variable.get('aws_conn_id', None)
        )

        # step_checker = EmrStepSensor(
        #     task_id='watch_step_bronze',
        #     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
        #     step_id="{{ task_instance.xcom_pull(task_ids='processing-bronze.emr_process_bronze', key='step_id') }}",
        #     aws_conn_id=Variable.get('aws_conn_id', None)
        # )
        s3_list = S3ListPrefixesOperator(
                    task_id="s3_list",
                    bucket=f"br-youse-data-lake-partitioner-{environment}",
                    prefix=f"datalakehouse/bronze/parquet/",
                    delimiter="/",
                    aws_conn_id=Variable.get('aws_conn_id', None)
                )
        
        crawler_bronze = PythonOperator(
            task_id='crawler_bronze',
            python_callable=crawler_s3,
            op_kwargs={
                'layer': 'bronze',
                'format': 'parquet',
                'prefix': None,
                'path': None
            },
        )

        [emr_process_bronze >> step_checker >> s3_list >> crawler_bronze]
    
    bronze_end = EmptyOperator(task_id="bronze_end")
    
    # with TaskGroup(group_id='processing-silver-endorsement') as tg_silver_endorsement:

    #     life_approved = EmrExecuteStepOperator(
    #         task_id='life_approved',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/endorsement/life_approved.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=webapp.life.endorsement.approved"
    #         ],
    #         step_name="silver_endorsement_life_approved",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )

    #     watch_step_life_approved = EmrStepSensor(
    #         task_id='watch_step_life_approved',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-endorsement.life_approved', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     life_approved >> [watch_step_life_approved]

    # with TaskGroup(group_id='processing-silver-pricing') as tg_silver_pricing:

    #     auto_pricing = EmrExecuteStepOperator(
    #         task_id='auto_pricing',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/pricing/auto_pricing.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=pricing-engine.bra.auto.pricing.created.historical,pricing-engine.bra.auto.pricing.created,pricing-engine.bra.auto.pricing.updated"
    #         ],
    #         step_name="silver_auto_pricing",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_auto_pricing = EmrStepSensor(
    #         task_id='watch_step_auto_pricing',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-pricing.auto_pricing', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     home_pricing = EmrExecuteStepOperator(
    #         task_id='home_pricing',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/pricing/home_pricing.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=pricing-engine.bra.home.pricing.created,pricing-engine.bra.home.pricing.updated,pricing-engine.bra.home.pricing.created.historical"
    #         ],
    #         step_name="silver_home_pricing",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_home_pricing = EmrStepSensor(
    #         task_id='watch_step_home_pricing',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-pricing.home_pricing', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     life_pricing = EmrExecuteStepOperator(
    #         task_id='life_pricing',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/pricing/life_pricing.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=pricing-engine.bra.life.pricing.created,pricing-engine.bra.life.pricing.updated,pricing-engine.bra.life.pricing.created.historical"
    #         ],
    #         step_name="silver_life_pricing",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_life_pricing = EmrStepSensor(
    #         task_id='watch_step_life_pricing',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-pricing.life_pricing', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     [
    #         auto_pricing >> watch_step_auto_pricing,
    #         home_pricing >> watch_step_home_pricing,
    #         life_pricing >> watch_step_life_pricing
    #     ]

    # with TaskGroup(group_id='processing-silver-policy') as tg_silver_policy:

    #     policy_version = EmrExecuteStepOperator(
    #         task_id='policy_version',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/policy/policy_version.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=policy-service.bra.life.policy-version.created"
    #         ],
    #         step_name="silver_policy_version",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_policy_version = EmrStepSensor(
    #         task_id='watch_step_policy_version',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-policy.policy_version', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     [
    #         policy_version >> watch_step_policy_version
    #     ]

    # with TaskGroup(group_id='processing-silver-zendesk') as tg_silver_zendesk:
        
    #     zendesk_ticket = EmrExecuteStepOperator(
    #         task_id='zendesk_ticket',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/zendesk/ZendeskTicketBase.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=zendesk.ticket.imported.v3"
    #         ],
    #         step_name="silver_zendesk_ticket",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_zendesk_ticket = EmrStepSensor(
    #         task_id='watch_step_zendesk_ticket',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-zendesk.zendesk_ticket', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     zendesk_chat = EmrExecuteStepOperator(
    #         task_id='zendesk_chat',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/zendesk/ZendeskChatBase.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=zendesk.chat.imported.v3"
    #         ],
    #         step_name="silver_zendesk_chat",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_zendesk_chat = EmrStepSensor(
    #         task_id='watch_step_zendesk_chat',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-zendesk.zendesk_chat', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     zendesk_user = EmrExecuteStepOperator(
    #         task_id='zendesk_user',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         execute_file=f"s3://youse-emr-assets-{environment}/python-libraries/src/datalakehouse/silver/dataframes/zendesk/ZendeskUserBase.py",
    #         paths="{{ task_instance.xcom_pull('return_processing_files', key='bronze_paths') }}",
    #         environment=environment,
    #         step_args=[
    #             '--AWS_VALIDATES=True',
    #             "--TEMP=True",
    #             f"--SOURCE_PATH=s3a://br-youse-data-lake-partitioner-{environment}/datalakehouse/bronze/",
    #             "--ROUTING_KEYS=zendesk.user.imported.v2"
    #         ],
    #         step_name="silver_zendesk_user",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     watch_step_zendesk_user = EmrStepSensor(
    #         task_id='watch_step_zendesk_user',
    #         job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #         step_id="{{ task_instance.xcom_pull(task_ids='processing-silver-zendesk.zendesk_user', key='return_value') }}",
    #         aws_conn_id=Variable.get('aws_conn_id', None)
    #     )
    #     crawler_silver_zendesk = PythonOperator(
    #         task_id='crawler_silver_zendesk',
    #         python_callable=crawler_s3,
    #         op_kwargs={
    #             'layer': 'silver',
    #             'format': 'delta'
    #         },
    #     )
    #     [
    #         zendesk_ticket >> watch_step_zendesk_ticket >> crawler_silver_zendesk,
    #         zendesk_chat >> watch_step_zendesk_chat >> crawler_silver_zendesk,
    #         zendesk_user >> watch_step_zendesk_user >> crawler_silver_zendesk,
    #     ]

    # with TaskGroup(group_id='processing-gold-zendesk') as tg_gold_zendesk:
    #     zendesk = DummyOperator(task_id='zendesk')
    #     zendesk_t1 = DummyOperator(task_id='zendesk_t1')
    #     zendesk_t2 = DummyOperator(task_id='zendesk_t2')
    #     zendesk_t3 = DummyOperator(task_id='zendesk_t3')

    #     zendesk >> [zendesk_t1, zendesk_t2, zendesk_t3]


    # s3_list_gold = S3ListPrefixesOperator(
    #     task_id="s3_list_gold",
    #     bucket=f"br-youse-data-lake-partitioner-{environment}",
    #     prefix="datalakehouse/gold/delta/",
    #     delimiter="/",
    #     aws_conn_id=Variable.get('aws_conn_id', None)
    # )
    
    # crawler_s3_gold = PythonOperator(
    #     task_id='crawler_s3_gold',
    #     python_callable=crawler_s3,
    #     op_kwargs={
    #         'layer': 'gold',
    #         'format': 'delta'
    #     },
    # )

    # terminate_cluster = EmrTerminateJobFlowOperator(
    #     task_id='terminate_emr_cluster',
    #     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', dag_id='dag_invoke_emr', key='return_value') }}",
    #     aws_conn_id=Variable.get('aws_conn_id', None)
    # )

    # gold = DummyOperator(task_id="gold", dag=dag)

    # run_end = EmptyOperator(task_id="End")

    run_start >> tg_pre_processing >> cluster_creator >> wait_emr_cluster >> tg_bronze >> bronze_end
    # bronze_end >> [tg_silver_endorsement, tg_silver_pricing, tg_silver_policy, tg_silver_zendesk] >> run_end
    # bronze_end >> [tg_silver_zendesk] >> run_end