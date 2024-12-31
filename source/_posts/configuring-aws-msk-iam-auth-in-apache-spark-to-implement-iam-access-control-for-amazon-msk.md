---
title: 在 Apache Spark 中設定 aws-msk-iam-auth 以實現 Amazon MSK 的 IAM 存取控制
date: 2024-12-31
tags: [aws, apache, spark, iam, msk, kafka]
---

## 前言

AWS 的 MSK (Managed Streaming for Apache Kafka) 的 [官方文件](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html) 有提到該如何使用 Amazon IAM 取得操作 MSK 的權限，其中的重點是

1. [aws-msk-iam-auth](https://github.com/aws/aws-msk-iam-auth) 函式庫，可以在 [Github releases](https://github.com/aws/aws-msk-iam-auth/releases) 或 [Maven repo](https://mvnrepository.com/artifact/software.amazon.msk/aws-msk-iam-auth) 下載
2. 客戶端設定

    ```properties
    security.protocol=SASL_SSL
    sasl.mechanism=AWS_MSK_IAM
    sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
    sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
    ```

但是翻了許多 AWS 官方文件都沒有特別提到 Apache Spark 怎麼使用這個函式庫

## 在 Apache Spark 中設定 aws-msk-iam-auth 以實現 Amazon MSK 的 IAM 存取控制

其實只要參考上述的客戶端設定然後微調即可

1. 首先一定要有函示庫，你可以先手動下載好 JAR 檔之後再在 `spark-submit` 的時候加上 `--conf spark.jars=<path-toaws-msk-iam-auth>`；或直接在 `spark-submit` 的時候使用 `--packages software.amazon.msk:aws-msk-iam-auth:2.2.0`。

2. 取得 JAR 檔之後還要在讀寫 Kafka topic 的時候設定，如下

```python
KAFKA_BOOTSTRAP_SERVERS = "host1:port1,host2:port2"
KAFKA_TOPIC = "KAFKA_TOPIC"

# Streaming Read from kafka topic
df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    .load()
)
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Batch write to kafka topic
(
    df
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", KAFKA_TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    .save()
)
```

## 碎碎念

在測試 Spark 讀寫 MSK 的時候一直噴出 Timeout Error: `org.apache.kafka.common.errors.TimeoutException (Topic topic12345 not present in metadata after 60000 ms.)`，還以為是網路問題 (Security Group) 沒開好，結果卻是因為權限設定沒處理好 (因為一開始以為只要 InstanceProfile 的權限處理好，剩下就自己會通了)
