from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

class EmrListStepSensorOperator(BaseOperator):
    """
    Get streaming run id in databricks api
    """
    template_fields = ('job_flow_id', 'list_step_id')

    @apply_defaults
    def __init__(self, job_flow_id, list_step_id, aws_conn_id, **kwargs):

        super(EmrListStepSensorOperator, self).__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.list_step_id = list_step_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):

        for step_id in self.list_step_id:
            print(step_id)

            response = EmrStepSensor(
                task_id=f'watch_step_{step_id}',
                job_flow_id=self.job_flow_id,
                step_id=step_id,
                aws_conn_id=self.aws_conn_id
            )
            print(response)
            context["ti"].xcom_push(key=f'get_waiter_{step_id.lower()}', value=str(response))

class EmrStepSensorOperator(BaseOperator):
    """
    Get streaming run id in databricks api
    """
    template_fields = ('job_flow_id', 'step_id')

    @apply_defaults
    def __init__(self, job_flow_id, step_id, aws_conn_id, **kwargs):

        super(EmrStepSensorOperator, self).__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):

        response = EmrStepSensor(
            job_flow_id=self.job_flow_id,
            step_id=self.step_id,
            aws_conn_id=self.aws_conn_id
        )
        print(response)
        context["ti"].xcom_push(key=f'get_waiter_{self.step_id.lower()}', value=str(response))