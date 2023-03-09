from __future__ import annotations
from typing import TYPE_CHECKING, Any, Sequence
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.links.emr import EmrClusterLink
from airflow.utils.helpers import prune_dict
from airflow.exceptions import AirflowException
from airflow.compat.functools import cached_property

class EmrCreateClusterOperator(BaseOperator):
    """
    Create Emr cluster and wait for status ready
    """
    template_fields: Sequence[str] = ('waiter_delay', 'waiter_max_attempts')

    @apply_defaults
    def __init__(self, 
                 aws_conn_id: str = "aws_default", 
                 emr_conn_id: str | None = "emr_default", 
                 wait_for_completion: bool = True, 
                 waiter_delay: int = 10, 
                 waiter_max_attempts: int = 500, 
                 **kwargs):

        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    @cached_property
    def _emr_hook(self) -> AwsGenericHook:
        """Create and return an AwsGenericHook."""
        return AwsGenericHook(
            aws_conn_id=self.aws_conn_id, client_type="emr"
        )
    
    def execute(self, context):
        self.log.info(
            "Creating job flow using aws_conn_id: %s, emr_conn_id: %s", self.aws_conn_id, self.emr_conn_id
        )
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
        config = {}
        config.update(JOB_FLOW_OVERRIDES)
        response = self._emr_hook.get_conn().run_job_flow(**config)
        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException(f"Job flow creation failed: {response}")
        else:
            self._job_flow_id = response["JobFlowId"]
            self.log.info("Job flow with id %s created", self._job_flow_id)
            EmrClusterLink.persist(
                context=context,
                operator=self,
                region_name=self._emr_hook.conn_region_name,
                aws_partition=self._emr_hook.conn_partition,
                job_flow_id=self._job_flow_id,
            )

            if self.wait_for_completion:
                self._emr_hook.get_conn().get_waiter("cluster_running").wait(
                    ClusterId=self._job_flow_id,
                    WaiterConfig=prune_dict(
                        {
                            "Delay": self.waiter_delay,
                            "MaxAttempts": self.waiter_max_attempts,
                        }
                    ),
                )

            return self._job_flow_id