---
title: 使用 Apache Spark 執行 Apache Iceberg 的 Schema Merge
date: 2025-02-08
tags: [ApacheSpark, ApacheIceberg]
---

## 小雷

在依照官方文件 [Spark-Writes Schema Merge](https://iceberg.apache.org/docs/latest/spark-writes/#schema-merge) 中即使是設定了資料表參數和確定使用 Apache Spark 開始寫資料表的時候你依然會遇到 `xxxx is out of order, before createdby`，可參考 [Issue: 8908](https://github.com/apache/iceberg/issues/8908)

## 解法

也正如 Issue 中最後所講的，我們需要額外設定 Apache Spark 參數: `spark.sql.iceberg.check-ordering=false` 即可略過這項檢查導致的錯誤

## 完整操作範例

1. 使用 Apache Spark 建立資料表或更新資料表參數

    > IRC means Iceberg REST Catalog

    ```sql
    -- Following is SparkSQL sample
    CREATE TABLE irc.default.sample (
        col_int int,
        col_str str
    )
    USING iceberg
    TBLPROPERTIES (
        'write.spark.accept-any-schema'='true'
    );

    -- OPTIONAL, if you had set this property during table creation
    ALTER TABLE irc.default.sample SET TBLPROPERTIES(
        'write.spark.accept-any-schema'='true'
    )
    ```

2. 使用 Apache Spark 寫入資料表

    ```python
    catalog_name = "irc"
    aws_account_id = "123456789012"
    aws_region = "us-east-1"
    spark = (
        SparkSession.builder
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensionIcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", catalog_name)
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", f"https://glue.{aws_region}.amazonaws.coiceberg") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{aws_account_id}")
        .config(f"spark.sql.catalog.{catalog_name}.rest.sigv4-enabled", "true")
        .config(f"spark.sql.catalog.{catalog_name}.rest.signing-name", "glue")
        ...
        config("spark.sql.iceberg.check-ordering", "false")
    ).getOrCreate()

    ...

    data.writeTo("irc.default.sample").option("mergeSchema","true").append()
    ```
