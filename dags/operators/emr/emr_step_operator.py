from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.helpers import prune_dict
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.compat.functools import cached_property

# Se cluster mode consegue uma concorrencia maior de acordo com a quantidade de nós comparado com o client mode
EMR_DEPLOY_MODE = Variable.get('EMR__DEPLOY__MODE', "client")
EMR_MAX_PATH = Variable.get('EMR__MAX__PATH', 30)

class EmrExecuteStepOperator(BaseOperator):
    """
    Add Emr step and wait for termination
    """
    template_fields = ('job_flow_id', 'paths')

    @apply_defaults
    def __init__(self, job_flow_id, execute_file, paths, environment, step_args, step_name, aws_conn_id, wait_for_completion: bool = True, waiter_delay: int = 10, waiter_max_attempts: int = 500,  **kwargs):

        super(EmrExecuteStepOperator, self).__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.execute_file = execute_file
        self.paths = paths
        self.environment = environment
        self.step_args = step_args
        self.step_name = step_name
        self.aws_conn_id = aws_conn_id
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

        def add_step(environment, execute_file, path_list, step_args, step_name):

            if path_list:
                custom_args = [
                    "spark-submit", "--master", "yarn",
                    "--deploy-mode", EMR_DEPLOY_MODE,
                    execute_file,
                    f"--PATHS={path_list}",
                    f"--ENVIRONMENT={environment}",
                ]
            else:
                custom_args = [
                    "spark-submit", "--master", "yarn",
                    "--deploy-mode", EMR_DEPLOY_MODE,
                    execute_file,
                    f"--ENVIRONMENT={environment}",
                ]
            custom_args.extend(step_args)
            
            add_step = {
                "Name": step_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {  
                    "Jar": "command-runner.jar",
                    "Args": custom_args
                }
            }
            return add_step
        SPARK_STEPS = []
        if self.paths:
            path_list = []
            for path in self.paths:
                path_list.append(path)
                if len(path_list) == EMR_MAX_PATH:
                    SPARK_STEPS.append(add_step(self.environment, self.execute_file, path_list, self.step_args, self.step_name))
                    path_list.clear()
            if path_list:
                SPARK_STEPS.append(add_step(self.environment, self.execute_file, path_list, self.step_args, self.step_name))
        else:
            path_list = []
            SPARK_STEPS.append(add_step(self.environment, self.execute_file, path_list, self.step_args, self.step_name))

        # EmrHook = AwsGenericHook( aws_conn_id=self.aws_conn_id, client_type="emr")
        config = {}
        response = self._emr_hook.get_conn().add_job_flow_steps(JobFlowId=self.job_flow_id, Steps=SPARK_STEPS, **config)

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Adding steps failed: {response}")

        self.log.info("Steps %s added to JobFlow", response["StepIds"])

        if self.wait_for_completion:
            waiter = self._emr_hook.get_conn().get_waiter("step_complete")
            for step_id in response["StepIds"]:
                waiter.wait(
                    ClusterId=self.job_flow_id,
                    StepId=step_id,
                    WaiterConfig=prune_dict(
                        {
                            "Delay": self.waiter_delay,
                            "MaxAttempts": self.waiter_max_attempts,
                        }
                    ),
                )
        context["ti"].xcom_push(key='return_value', value=str(response["StepIds"]))