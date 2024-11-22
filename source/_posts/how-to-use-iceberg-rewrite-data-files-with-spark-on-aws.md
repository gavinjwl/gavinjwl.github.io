---
title: 如何在 AWS 上使用 Spark 執行 Iceberg rewrite_data_files
date: 2024-11-22
tags: [AWS, EMR-Serverless, Glue, ApacheSpark, ApacheIceberg, Compaction]
---

## How to Use Iceberg rewrite_data_files with Spark on AWS

本文主要針對在 AWS 上使用 AWS Glue 和 Amazon EMR Serverless 的 Apache Spark 執行 Apache Iceberg [rewrite_data_files](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files) 程序的時候會遇到的 __No space left on device__ 進行疑難排解。以下是 Iceberg/Spark rewrite_data_files 的 Glue/Spark 測試範例 (特別設定了 `rewrite-all` 來進行測試，生產環境的話則應該不需要此參數)。

```python
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf, SparkContext

options = ["JOB_NAME"]
args = getResolvedOptions(sys.argv, options)

catalog_nm = "glue_catalog"
s3_warehouse_location = "s3://"
conf = SparkConf()
conf.set(f"spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{catalog_nm}.warehouse", s3_warehouse_location)
conf.set(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# https://iceberg.apache.org/docs/latest/spark-procedures/#general-options
catalog_nm = "glue_catalog"
database_name = "default"
table_name = "sample_table"

target_file_size_bytes = 1024*1024*1024*1 # 1 GB, default is 512 MB
max_file_group_size_bytes = 1024*1024*1024*10 # 10 GB, default is 100 GB
max_concurrent_file_group_rewrites = 10 # depends on the number of executor

spark.sql(f'''
CALL {catalog_nm}.system.rewrite_data_files(
    table => '{database_name}.{table_name}',
    strategy => 'sort',
    sort_order => 'zorder(col_1, col_2, col_3)',
    options => map(
        'target-file-size-bytes', '{target_file_size_bytes}',
        'max-file-group-size-bytes', '{max_file_group_size_bytes}',
        'max-concurrent-file-group-rewrites', '{max_concurrent_file_group_rewrites}',
        'rewrite-all', 'true'
    )
)
''').show()

job.commit()
```

## Why No space left on device during rewriting

由於尚未找到相關文件在描述 Iceberg/Spark 在 rewrite 時期的細節，但是經由錯誤訊息、硬碟觀察和 Spark UI 可以發現在執行的時候 Apache Spark 會將資料暫存 (cache)，所以如果記憶體或硬碟不夠大的話就會跑出這個錯誤訊息。

解決 __No space left on device__ 的方法可以是:

1. 一直加機器
2. 用更大的硬碟或比較便宜的 Amazon S3 來暫存 (本益比高)

## AWS Glue

AWS Glue 使用 [Amazon S3 當作 Shuffle storage](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-shuffle-manager.html) ([部落格文章](https://aws.amazon.com/tw/blogs/big-data/introducing-amazon-s3-shuffle-in-aws-glue/))。以下是啟用 Amazon S3 當作 Shuffle storage 後的 Glue Job 設定範例: `aws glue get-job`，要注意的就是範例中的 `"--write-shuffle-files-to-s3": "true"`

```json
{
    "Job": {
        "Name": "sample-job",
        "JobMode": "SCRIPT",
        "JobRunQueuingEnabled": false,
        "Description": "",
        "Role": "xxxxxxxxxx",
        "CreatedOn": "xxxxxxxxxx",
        "LastModifiedOn": "xxxxxxxxxx",
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": "s3://aws-glue-assets-account_id-region_code/scripts/sample-job.py",
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://aws-glue-assets-account_id-region_code/sparkHistoryLogs/",
            "--enable-job-insights": "true",
            "--enable-observability-metrics": "true",
            "--enable-glue-datacatalog": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--datalake-formats": "iceberg",
            "--job-language": "python",
            "--TempDir": "s3://aws-glue-assets-account_id-region_code/temporary/",
            "--write-shuffle-files-to-s3": "true"
        },
        "MaxRetries": 0,
        "AllocatedCapacity": 10,
        "Timeout": 2880,
        "MaxCapacity": 10.0,
        "WorkerType": "G.1X",
        "NumberOfWorkers": 10,
        "GlueVersion": "4.0",
        "ExecutionClass": "STANDARD"
    }
}
```

## Amazon EMR Serverless

在 Amazon EMR Serverless 則提供了 [Shuffle 專用的硬碟](https://aws.amazon.com/tw/about-aws/whats-new/2024/05/amazon-emr-serverless-shuffle-optimized-disks/)，具體設定可以參考[官方文件](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-shuffle-optimized-disks.html)，以下是設定後的參考 `aws emr-serverless get-application`，需要注意的是要設定了 `"diskType": "SHUFFLE_OPTIMIZED"` 之後才能調大 disk。

```json
{
    "application": {
        "applicationId": "xxxxxxxxxxxx",
        "name": "sample-application",
        "arn": "arn:aws:emr-serverless:region_code:account_id:/applications/xxxxxxxxxxxx",
        "releaseLabel": "emr-7.2.0",
        "type": "Spark",
        "state": "STOPPED",
        "stateDetails": "AUTO_STOPPING",
        "initialCapacity": {
            "Executor": {
                "workerCount": 2,
                "workerConfiguration": {
                    "cpu": "8 vCPU",
                    "memory": "60 GB",
                    "disk": "2000 GB",
                    "diskType": "SHUFFLE_OPTIMIZED"
                }
            },
            "Driver": {
                "workerCount": 1,
                "workerConfiguration": {
                    "cpu": "4 vCPU",
                    "memory": "30 GB",
                    "disk": "200 GB"
                }
            }
        },
        "maximumCapacity": {
            "cpu": "400 vCPU",
            "memory": "3000 GB",
            "disk": "100000 GB"
        },
        "createdAt": "masking",
        "updatedAt": "masking",
        "tags": {},
        "autoStartConfiguration": {
            "enabled": true
        },
        "autoStopConfiguration": {
            "enabled": true,
            "idleTimeoutMinutes": 90
        },
        "architecture": "X86_64",
        "runtimeConfiguration": [
            {
                "classification": "spark-defaults",
                "properties": {
                    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            }
        ],
        "monitoringConfiguration": {
            "managedPersistenceMonitoringConfiguration": {
                "enabled": true
            }
        },
        "interactiveConfiguration": {
            "studioEnabled": true,
            "livyEndpointEnabled": false
        }
    }
}
```
