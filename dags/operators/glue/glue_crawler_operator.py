# import json
# from airflow.models import BaseOperator
# from airflow.compat.functools import cached_property
# from airflow.utils.decorators import apply_defaults
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
# from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

# class GlueCrawlerExecuteOperator(BaseOperator):
#     """
#     Get streaming run id in databricks api
#     """

#     @apply_defaults
#     def __init__(self, bucket_name, prefix, s3_list, environment, format, layer, aws_conn_id, poll_interval: int = 5, wait_for_completion: bool = True, **kwargs):

#         super().__init__(**kwargs)
#         self.bucket_name = bucket_name
#         self.prefix = prefix
#         self.s3_list = s3_list
#         self.environment = environment
#         self.format = format
#         self.layer = layer
#         self.aws_conn_id = aws_conn_id
#         self.poll_interval = poll_interval
#         self.wait_for_completion = wait_for_completion
    
#     def execute(self, context):
#         """
#         Executes AWS Glue Crawler from Airflow
#         :return: the name of the current glue crawler.
#         """
#         full_paths, s3_target, delta_target = [], [], []
#         if self.s3_list:
#             S3hook = S3Hook(self.aws_conn_id)
#             for prefix in self.prefix:
#                 paths = S3hook.list_prefixes(
#                     bucket_name=self.bucket_name, 
#                     prefix=prefix, 
#                     delimiter="/"
#                 )
#                 for path in paths:
#                     full_paths.append(f's3://{self.bucket_name}/{path}')
#         else:
#             for path in self.prefix:
#                 full_paths.append(f's3://{self.bucket_name}/{path}')
        
#         for full_path in full_paths:
#             if self.format == 'parquet':
#                 s3_target.append( { "Path": full_path } )
#             elif self.format == 'delta':
#                 delta_target.append(full_path)

#         glue_crawler_config = {
#             "Name": f"datalake_{self.layer}_{self.environment}",
#             "DatabaseName": f"datalake_{self.layer}_{self.environment}",
#             "Description": "Crawler for generating datalake tables from s3",
#             "RecrawlPolicy": { 
#                 "RecrawlBehavior": 'CRAWL_EVERYTHING'
#             },
#             "Role": f"iam-glue-service-role-{self.environment}",
#             "SchemaChangePolicy": { 
#                 "DeleteBehavior": 'DEPRECATE_IN_DATABASE',
#                 "UpdateBehavior": 'UPDATE_IN_DATABASE'
#             },
#             "TablePrefix": "dlh_",
#             "Targets": { 
#                 "DeltaTargets": [
#                     {
#                         "CreateNativeDeltaTable": True,
#                         "DeltaTables": delta_target,
#                         "WriteManifest": False
#                     }
#                 ],
#                 "S3Targets": s3_target
#             }
#         }
#         GlueCrawlerOperator(
#             task_id=f"datalake_{self.layer}_{self.environment}",
#             config=glue_crawler_config,
#             wait_for_completion=self.wait_for_completion,
#             aws_conn_id=self.aws_conn_id,
#         )
            # response = CrawlerHook.create(
            #     Name=,
            #     Role=,
            #     DatabaseName=,
            #     Description="Crawler for generating datalake tables from s3",
            #     Targets=targets,
            #     TablePrefix='dlh_',
            #     SchemaChangePolicy={
            #         'UpdateBehavior': 'UPDATE_IN_DATABASE',
            #         'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            #     },
            #     RecrawlPolicy={
            #         'RecrawlBehavior': 'CRAWL_EVERYTHING'
            #     },
            #     LineageConfiguration={
            #         'CrawlerLineageSettings': 'DISABLE'
            #     }
            # )

            # print(json.dumps(response, indent=4, sort_keys=True, default=str))

        # context["ti"].xcom_push(key=f"datalake_{self.layer}_{self.environment}", value=full_path)

import sys
import warnings
from typing import TYPE_CHECKING
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property




class S3GlueCrawlerOperator(BaseOperator):
    """
    Creates, updates and triggers an AWS Glue Crawler. AWS Glue Crawler is a serverless
    service that manages a catalog of metadata tables that contain the inferred
    schema, format and data types of data stores within the AWS cloud.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerOperator`

    :param config: Configurations for the AWS Glue crawler
    :param aws_conn_id: aws connection to use
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :param wait_for_completion: Whether or not wait for crawl execution completion. (default: True)
    """
    ui_color = '#ededed'
    def __init__( self, bucket_name, prefix, s3_list, environment, layer, aws_conn_id='aws_default', poll_interval: int = 5, wait_for_completion: bool = True, **kwargs ):
        
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_list = s3_list
        self.environment = environment
        self.layer = layer

    @cached_property
    def hook(self) -> GlueCrawlerHook:
        """Create and return an GlueCrawlerHook."""
        return GlueCrawlerHook(self.aws_conn_id)

    def execute(self, context: 'Context'):
        """
        Executes AWS Glue Crawler from Airflow
        :return: the name of the current glue crawler.
        """
        full_paths, s3_target = [], []
        if self.s3_list:
            S3hook = S3Hook(self.aws_conn_id)
            for prefix in self.prefix:
                paths = S3hook.list_prefixes(
                    bucket_name=self.bucket_name, 
                    prefix=prefix, 
                    delimiter="/"
                )
                for path in paths:
                    full_paths.append(f's3://{self.bucket_name}/{path}')
        else:
            for path in self.prefix:
                full_paths.append(f's3://{self.bucket_name}/{path}')
        
        for full_path in full_paths:
            s3_target.append( { "Path": full_path } )

        glue_crawler_config = {
            "Name": f"datalake_{self.layer}_{self.environment}",
            "DatabaseName": f"datalake_{self.layer}_{self.environment}",
            "Description": "Crawler for generating datalake tables from s3",
            "RecrawlPolicy": { 
                "RecrawlBehavior": 'CRAWL_EVERYTHING'
            },
            "Role": f"iam-glue-service-role-{self.environment}",
            "SchemaChangePolicy": { 
                "DeleteBehavior": 'DEPRECATE_IN_DATABASE',
                "UpdateBehavior": 'UPDATE_IN_DATABASE'
            },
            "TablePrefix": "dlh_",
            "Targets": { 
                "S3Targets": s3_target
            }
        }
        crawler_name = glue_crawler_config['Name']
        if self.hook.has_crawler(crawler_name):
            self.hook.update_crawler(**glue_crawler_config)
        else:
            self.hook.create_crawler(**glue_crawler_config)

        self.log.info("Triggering AWS Glue Crawler")
        self.hook.start_crawler(crawler_name)
        if self.wait_for_completion:
            self.log.info("Waiting for AWS Glue Crawler")
            self.hook.wait_for_crawler_completion(crawler_name=crawler_name, poll_interval=self.poll_interval)

        return crawler_name
    
class DeltaGlueCrawlerOperator(BaseOperator):
    """
    Creates, updates and triggers an AWS Glue Crawler. AWS Glue Crawler is a serverless
    service that manages a catalog of metadata tables that contain the inferred
    schema, format and data types of data stores within the AWS cloud.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerOperator`

    :param config: Configurations for the AWS Glue crawler
    :param aws_conn_id: aws connection to use
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :param wait_for_completion: Whether or not wait for crawl execution completion. (default: True)
    """
    ui_color = '#ededed'
    def __init__( self, bucket_name, prefix, s3_list, environment, layer, aws_conn_id='aws_default', poll_interval: int = 5, wait_for_completion: bool = True, **kwargs ):

        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_list = s3_list
        self.environment = environment
        self.layer = layer

    @cached_property
    def hook(self) -> GlueCrawlerHook:
        """Create and return an GlueCrawlerHook."""
        return GlueCrawlerHook(self.aws_conn_id)

    def execute(self, context: 'Context'):
        """
        Executes AWS Glue Crawler from Airflow

        :return: the name of the current glue crawler.
        """
        full_paths, delta_target = [], []
        if self.s3_list:
            S3hook = S3Hook(self.aws_conn_id)
            for prefix in self.prefix:
                paths = S3hook.list_prefixes(
                    bucket_name=self.bucket_name, 
                    prefix=prefix, 
                    delimiter="/"
                )
                for path in paths:
                    full_paths.append(f's3://{self.bucket_name}/{path}')
        else:
            for path in self.prefix:
                full_paths.append(f's3://{self.bucket_name}/{path}')
        
        for full_path in full_paths:
            delta_target.append(full_path)

        glue_crawler_config = {
            "Name": f"datalake_{self.layer}_{self.environment}",
            "DatabaseName": f"datalake_{self.layer}_{self.environment}",
            "Description": "Crawler for generating datalake tables from s3",
            "RecrawlPolicy": { 
                "RecrawlBehavior": 'CRAWL_EVERYTHING'
            },
            "Role": f"iam-glue-service-role-{self.environment}",
            "SchemaChangePolicy": { 
                "DeleteBehavior": 'DEPRECATE_IN_DATABASE',
                "UpdateBehavior": 'UPDATE_IN_DATABASE'
            },
            "TablePrefix": "dlh_",
            "Targets": { 
                "DeltaTargets": [
                    {
                        "CreateNativeDeltaTable": True,
                        "DeltaTables": delta_target,
                        "WriteManifest": False
                    }
                ]
            }
        }
        crawler_name = glue_crawler_config['Name']
        if self.hook.has_crawler(crawler_name):
            self.hook.update_crawler(**glue_crawler_config)
        else:
            self.hook.create_crawler(**glue_crawler_config)

        self.log.info("Triggering AWS Glue Crawler")
        self.hook.start_crawler(crawler_name)
        if self.wait_for_completion:
            self.log.info("Waiting for AWS Glue Crawler")
            self.hook.wait_for_crawler_completion(crawler_name=crawler_name, poll_interval=self.poll_interval)

        return crawler_name