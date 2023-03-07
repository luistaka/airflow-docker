from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.emr import EmrHook

# Se cluster mode consegue uma concorrencia maior de acordo com a quantidade de nós comparado com o client mode
EMR_DEPLOY_MODE = Variable.get('EMR__DEPLOY__MODE', "client")
EMR_MAX_PATH = Variable.get('EMR__MAX__PATH', 30)

class EmrExecuteStepOperator(BaseOperator):
    """
    Get streaming run id in databricks api
    """
    template_fields = ('job_flow_id', 'paths')

    @apply_defaults
    def __init__(self, job_flow_id, execute_file, paths, environment, step_args, step_name, aws_conn_id, **kwargs):

        super(EmrExecuteStepOperator, self).__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.execute_file = execute_file
        self.paths = paths
        self.environment = environment
        self.step_args = step_args
        self.step_name = step_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):

        EmrClient = EmrHook(aws_conn_id=self.aws_conn_id)
        count = 0
        path_list = []
        SPARK_STEPS = []
        for path in self.paths:
            
            path_list.append(path)
            count += 1
            if count == EMR_MAX_PATH:
                custom_args = [
                    "spark-submit", "--master", "yarn",
                    "--deploy-mode", EMR_DEPLOY_MODE,
                    "--py-files", f"s3://youse-emr-assets-{self.environment}/python-libraries/src_lft_teste/youse-datapipeline.zip",
                    self.execute_file,
                    f"--PATHS={path_list}",
                    f"--ENVIRONMENT={self.environment}",
                ]
                custom_args.extend(self.step_args)
                add_step = {
                    "Name": self.step_name,
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {  
                        "Jar": "command-runner.jar",
                        "Args": custom_args
                    }
                }
                SPARK_STEPS.append(add_step)
                path_list.clear()
                count = 0

        if path_list:
            custom_args = [
                "spark-submit", "--master", "yarn",
                "--deploy-mode", EMR_DEPLOY_MODE,
                "--py-files", f"s3://youse-emr-assets-{self.environment}/python-libraries/src_lft_teste/youse-datapipeline.zip",
                self.execute_file,
                f"--PATHS={path_list}",
                f"--ENVIRONMENT={self.environment}",
            ]
            custom_args.extend(self.step_args)
            add_step = {
                "Name": self.step_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {  
                    "Jar": "command-runner.jar",
                    "Args": custom_args
                }
            }
            SPARK_STEPS.append(add_step)

        newstep = EmrClient.add_job_flow_steps(
            job_flow_id=self.job_flow_id,
            steps=SPARK_STEPS,
            wait_for_completion=True
        )

        context["ti"].xcom_push(key='step_id', value=str(newstep))