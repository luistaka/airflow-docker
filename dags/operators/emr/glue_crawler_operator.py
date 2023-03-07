from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

class GlueCrawlerExecuteOperator(BaseOperator):
    """
    Get streaming run id in databricks api
    """

    @apply_defaults
    def __init__(self, bucket_name, prefix, environment, format, aws_conn_id, **kwargs):

        super(GlueCrawlerExecuteOperator, self).__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.environment = environment
        self.format = format
        self.aws_conn_id = aws_conn_id

    def execute(self, context):

        hook = S3Hook(self.aws_conn_id)
        paths = hook.list_prefixes(
            bucket_name=self.bucket_name, 
            prefix=self.prefix, 
            delimiter="/"
        )

        s3_targets, delta_targets = [], [] 
        for path in paths:
            if self.format == 'parquet':
                s3_targets.append({"Path": f'{self.bucket_name}/{path}'})
            elif self.format == 'delta':
                delta_targets.append({"Path": f'{self.bucket_name}/{path}'})

        glue_crawler_config = {
            "Name": f"datalake_crawler-{self.layer}_{self.environment}",
            "Role": f"data-catalog-{self.environment}",
            "DatabaseName": f"datalake_{self.layer}_{self.environment}",
            "Description": "Crawler for generating datalake tables from s3",
            "Targets": {
                "S3Targets": s3_targets
            },
            "DeltaTargets": {
                "DeltaTables": delta_targets
            },
            "TablePrefix": 'dlh_',
            "SchemaChangePolicy": {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            "RecrawlPolicy": {
                'RecrawlBehavior': 'CRAWL_EVERYTHING'
            },
            "LineageConfiguration": {
                'CrawlerLineageSettings': 'DISABLE'
            },
            "Tags": {
                'datateam': 'data-engineer',
                'datalake-layer': f'{layer}'
            },
        }
        response = GlueCrawlerOperator(
            config=glue_crawler_config,
            aws_conn_id=self.aws_conn_id
        )

        context["ti"].xcom_push(key='return_value', value=str(response))