'''
export BUCKET=""
export PYSPARK_FILE=""

ICEBERG_VERSION="1.9.1"
SPARK_MAJOR_VERSION="3.5"
SPARK_VERSION="${SPARK_MAJOR_VERSION}.4"

spark-submit \
--packages org.apache.iceberg:iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12:${ICEBERG_VERSION}, \
    org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}, \
    org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}, \
    software.amazon.msk:aws-msk-iam-auth:2.2.0 \
s3://${BUCKET}/${PYSPARK_FILE} > log 2>&1 &

export PYSPARK_PYTHON=.venv/bin/python
nc -lk 9999
'''

#%%
import json
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import row_number, to_json, udf
from pyspark.sql.types import *

APP_NAME = "iceberg_spark_cdc_sample"
ACCOUNT_ID = "123456789012"
BUCKET_NAME = ""

DATABASE_NAME = "default"
SAMPLE_TABLE = "sample".lower()
SAMPLE_LONG_FORMAT_TABLE = SAMPLE_TABLE + "_long"

WAREHOUSE_PATH = f"s3://{BUCKET_NAME}/{DATABASE_NAME}/warehouse/"
CHECKPOINT_LOCATION = f"s3://{BUCKET_NAME}/{DATABASE_NAME}/sss-checkpoint/{APP_NAME}/"

CUSTOM_STREAM_APP = "custom.spark.streaming"
CUSTOM_CHECKPOINT_ID = "custom.streaming.snapshot_id"
CUSTOM_STREAMING_BRANCH = "custom_streaming_branch"

#%%
SAMPLE_SCHEMA = StructType([
    StructField("any_id", StringType(), True),
    StructField("array_col", ArrayType(StringType()), True),
    StructField("str_col", StringType(), True),
    StructField("int_col", IntegerType(), True),
    StructField("update_time", TimestampType(), True),
])

SAMPLE_EVENT_SCHEMA = StructType([
    StructField("_action", IntegerType(), True),
    StructField("any_id", StringType(), True),
    StructField("array_col", ArrayType(StringType()), True),
    StructField("str_col", StringType(), True),
    StructField("int_col", IntegerType(), True),
    StructField("update_time", TimestampType(), True),
])

SAMPLE_LONG_FORMAT_SCHEMA = StructType([
    StructField("any_id", StringType(), True),
    StructField("col_name", StringType(), True),
    StructField("col_value", StringType(), True),
    StructField("update_time", TimestampType(), True),
])

TABLE_ACTION_INSERT = 0
TABLE_ACTION_UPDATE = 1
TABLE_ACTION_DELETE = 2
CHANGE_LOG_SCHEMA = StructType([
    StructField("_action", IntegerType(), True),
    StructField("any_id", StringType(), True),
    StructField("col_name", StringType(), True),
    StructField("col_value", StringType(), True),
    StructField("update_time", TimestampType(), True),
])

#%%
def read_kafka(spark: SparkSession):
    KAFKA_BOOTSTRAP_SERVERS = ""
    KAFKA_TOPIC = ""
    return (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        .option("startingOffsets", "latest")  # latest or earliest
        # .option("minPartitions", str(min(24, 32) * 5)) ## performance tuning
        # .option("maxOffsetsPerTrigger", str(2000000 * 4)) ## performance tuning or debugging
        # .option("auto.offset.reset", "true") ## debugging
        # .option("failOnDataLoss", "false") ## debugging
        .load()
        .selectExpr("CAST(value AS STRING) as value")
    )

def read_socket(spark: SparkSession):
    return (
        spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

    )

#%%
@udf(returnType=SAMPLE_EVENT_SCHEMA)
def parsing_event(data: str):
    '''
    NOTE: parse_json will be offered in Spark 4.0.0.
    NOTE: Columns in output_data MUST have the same ordering as in SAMPLE_EVENT_SCHEMA
    '''
    try:
        json_data: dict = json.loads(data)
        output_data = dict()
        output_data["_action"] = json_data.get("_action", None)
        if output_data["_action"] == TABLE_ACTION_DELETE:
            # Since this is a deletion instruction, we need to ensure that each column is not None
            output_data["any_id"] = json_data["any_id"]
            output_data["array_col"] = json_data.get("array_col", [])
            output_data["str_col"] = json_data.get("str_col", "")
            output_data["int_col"] = json_data.get("int_col", 1)
            output_data["update_time"] = datetime.fromtimestamp(json_data["update_time"])
        else:
            output_data["any_id"] = json_data["any_id"]
            output_data["array_col"] = json_data.get("array_col", None)
            output_data["str_col"] = json_data.get("str_col", None)
            output_data["int_col"] = json_data.get("int_col", None)
            output_data["update_time"] = datetime.fromtimestamp(json_data["update_time"])
        return Row(**output_data)
    except:
        # TODO: to be verified by real data
        pass

#%%
def get_table_property(spark: SparkSession, database, table, key) -> str:
    result = spark.sql(f"SHOW TBLPROPERTIES {database}.{table}").filter(col("key") == lit(key))
    # result.show() ## DEBUGGING
    result = result.take(1)
    return str(result[0]["value"]) if len(result) > 0 else None

def set_table_property(spark: SparkSession, database, table, key, value):
    spark.sql(f"""ALTER TABLE {database}.{table} SET TBLPROPERTIES (
        '{key}'='{value}'
    )""")

def get_latest_snapshot_id(spark: SparkSession, database, table) -> int:
    snaphost_id = spark.sql(
        f"""
        SELECT snapshot_id
        FROM spark_catalog.{database}.{table}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1;
        """
    ).take(1)
    return snaphost_id[0]["snapshot_id"] if len(snaphost_id) > 0 else None

#%%
def merge_to_long_table(df: DataFrame):
    print("******* merge_to_long_table *******") ## DEBUGGING
    ## Apply De-duplication logic on input data, to pickup latest record based on update_time
    window_spec = Window.partitionBy(
        col("any_id"), col("col_name")
    ).orderBy(col("update_time").desc())
    deduplicated = df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

    # deduplicated.orderBy(["any_id", "col_name", "update_time"]).show(truncate=False) ## DEBUGGING

    ## Register the deduplicated input as temporary table to use in Iceberg Spark SQL statements
    TEMP_VIEW_NAME = "deduplicated"
    deduplicated.createOrReplaceTempView(TEMP_VIEW_NAME)

    compound_key = ["any_id", "col_name"]
    action_col = "_action"
    value_col = "col_value"
    timestamp_col = "update_time"

    ## Perform merge operation on incremental and deduplicated input data with MERGE INTO.
    query = f"""
    MERGE INTO spark_catalog.{DATABASE_NAME}.{SAMPLE_LONG_FORMAT_TABLE} t
    USING (
        SELECT
            {", ".join([field_name for field_name in CHANGE_LOG_SCHEMA.fieldNames()])}
        FROM {TEMP_VIEW_NAME}
    ) s
    ON {" AND ".join(["t.{col_name} = s.{col_name}".format(col_name=c_key) for c_key in compound_key])}
    WHEN MATCHED AND s.{timestamp_col} >= t.{timestamp_col} AND s.{action_col} = {TABLE_ACTION_DELETE} THEN DELETE
    WHEN MATCHED AND s.{timestamp_col} >= t.{timestamp_col} AND s.{action_col} != {TABLE_ACTION_DELETE}
        THEN UPDATE SET t.{value_col} = s.{value_col}, t.{timestamp_col} = s.{timestamp_col}
    WHEN NOT MATCHED AND s.{action_col} != {TABLE_ACTION_DELETE}
        THEN INSERT ({", ".join([col_name for col_name in SAMPLE_LONG_FORMAT_SCHEMA.fieldNames()])})
        VALUES ({", ".join(["s.{}".format(col_name) for col_name in SAMPLE_LONG_FORMAT_SCHEMA.fieldNames()])})
    """
    # print(query) ## DEBUGGING
    deduplicated.sparkSession.sql(query)

def create_changelog_view(spark: SparkSession, changelog_view, start_snapshot_id, end_snapshot_id):
    '''
    Iceberg changelog view: https://iceberg.apache.org/docs/latest/spark-procedures/#create_changelog_view
    '''
    start_snapshot_id = f"'start-snapshot-id', '{start_snapshot_id}'," if start_snapshot_id else ""
    end_snapshot_id = f"'end-snapshot-id', '{end_snapshot_id}'" if end_snapshot_id else ""
    query = f"""
    CALL spark_catalog.system.create_changelog_view(
        changelog_view => "{changelog_view}",
        table => '{DATABASE_NAME}.{SAMPLE_LONG_FORMAT_TABLE}',
        identifier_columns => array('any_id', 'col_name'),
        options => map(
            {start_snapshot_id}
            {end_snapshot_id}
        ),
        compute_updates => false
    )
    """
    spark.sql(query)

def changed_data_capture(spark: SparkSession, changed_compound_key: list[str], start_snapshot_id, end_snapshot_id):
    print("******* changed_data_capture *******") ## DEBUGGING
    # Create changelog_view
    changelog_view = f"{SAMPLE_LONG_FORMAT_TABLE}_clv"
    create_changelog_view(spark, changelog_view, start_snapshot_id, end_snapshot_id)
    changelog_view = spark.sql(f"select * from {changelog_view}")
    # changelog_view.orderBy(["any_id", "col_name", "_change_type"]).show() ## DEBUGGING

    # Get changed (inserted, updated or deleted) records
    changed_records = changelog_view.select(changed_compound_key).dropDuplicates()
    # changed_records.show() ## DEBUGGING

    return changed_records

def merge_to_wide_table(spark: SparkSession, changed_records: DataFrame, changed_compound_key: list[str]):
    print("******* merge_to_wide_table *******") ## DEBUGGING
    # Get upserted records and transform to wide-format
    col_values = ["array_col", "str_col", "int_col",]
    upserted_records = (
        # Get latest status according to compound key
        changed_records
        .join(
            spark.table(f"spark_catalog.{DATABASE_NAME}.{SAMPLE_LONG_FORMAT_TABLE}"), on=changed_compound_key, how="left"
        )
        # Get latest updateTime according to compound key
        .withColumn("update_time", spark_max("update_time").over(Window.partitionBy(changed_compound_key).orderBy(col("update_time").desc())))
        # Transform back to wide-format
        .groupBy(changed_compound_key + ["update_time"])
        .pivot("col_name", col_values).agg({"col_value": "first"})
        # Change data type
        .withColumn("array_col", from_json("array_col", ArrayType(StringType())))
        .withColumn("int_col", col("int_col").cast(IntegerType()))
    )
    # upserted_records.orderBy(changed_compound_key).show() ## DEBUGGING

    ### Leverage branch as Transaction
    table_identifier = f"spark_catalog.{DATABASE_NAME}.{SAMPLE_TABLE}"
    # Start Transaction
    spark.sql(f"""
        ALTER TABLE {table_identifier} CREATE OR REPLACE BRANCH `{CUSTOM_STREAMING_BRANCH}`
        RETAIN 1 DAYS
    """)
    # spark.sql(f"SELECT * FROM {table_identifier}.refs").show() ## DEBUGGING

    ## Delete branch data from changed_records
    source_view = "source_view"
    changed_records.createOrReplaceTempView(source_view)
    delete_query = f"""
    DELETE FROM {table_identifier}.branch_{CUSTOM_STREAMING_BRANCH} AS t
    WHERE EXISTS (
        SELECT {", ".join(changed_compound_key)}
        FROM {source_view}
        WHERE {" AND ".join([
            "t.{col_name} = {col_name}".format(col_name=c_key) for c_key in changed_compound_key
        ])}
    )
    """
    # print(delete_query) ## DEBUGGING
    spark.sql(delete_query)

    # Insert upserted_records into branch
    upserted_records.writeTo(f"{table_identifier}.branch_{CUSTOM_STREAMING_BRANCH}").append()

    # END Transaction
    ff = spark.sql(f"CALL spark_catalog.system.fast_forward('{table_identifier}', 'main', '{CUSTOM_STREAMING_BRANCH}')")
    # ff.show() ## DEBUGGING
    # spark.sql(f"ALTER TABLE {table_identifier} DROP BRANCH `{CUSTOM_STREAMING_BRANCH}`")

def batch_function(df: DataFrame, batch_id):
    print(f"############# {batch_id} ################") ## DEBUGGING
    start_snapshot_id = get_table_property(df.sparkSession, DATABASE_NAME, SAMPLE_LONG_FORMAT_TABLE, CUSTOM_CHECKPOINT_ID)
    print(f"start_snapshot_id: {start_snapshot_id}") ## DEBUGGING

    merge_to_long_table(df)

    end_snapshot_id = get_latest_snapshot_id(df.sparkSession, DATABASE_NAME, SAMPLE_LONG_FORMAT_TABLE)
    print(f"end_snapshot_id: {end_snapshot_id}") ## DEBUGGING

    changed_compound_key = ["any_id"]
    changed_data = changed_data_capture(df.sparkSession, changed_compound_key, start_snapshot_id, end_snapshot_id)

    merge_to_wide_table(df.sparkSession, changed_data, changed_compound_key)

    set_table_property(df.sparkSession, DATABASE_NAME, SAMPLE_LONG_FORMAT_TABLE, CUSTOM_CHECKPOINT_ID, end_snapshot_id)

def main():
    spark = (
        SparkSession.Builder()
        .appName(APP_NAME)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.catalog.spark_catalog.type", "glue")
        .config("spark.sql.catalog.spark_catalog.glue.id", ACCOUNT_ID)
        .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.iceberg.check-ordering", "false")
        .config("spark.sql.caseSensitive", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") ## DEBUGGING
        .getOrCreate()
    )

    print(f"SET PROPERTIES spark_catalog.{DATABASE_NAME}.{SAMPLE_LONG_FORMAT_TABLE}")
    spark.sql(f"""ALTER TABLE spark_catalog.{DATABASE_NAME}.{SAMPLE_LONG_FORMAT_TABLE} SET TBLPROPERTIES (
        '{CUSTOM_STREAM_APP}'='{APP_NAME}'
    )""")
    print(f"SET PROPERTIES spark_catalog.{DATABASE_NAME}.{SAMPLE_TABLE}")
    spark.sql(f"""ALTER TABLE spark_catalog.{DATABASE_NAME}.{SAMPLE_TABLE} SET TBLPROPERTIES (
        '{CUSTOM_STREAM_APP}'='{APP_NAME}'
    )""")

    input = (
        read_kafka(spark)
        .filter(col("value").isNotNull())
        .select(parsing_event("value").alias("data"))
        .select("data.*")
        .withColumn("array_col", to_json(col("array_col")))
        .withColumn("int_col", col("int_col").cast(StringType()))
    )

    compound_key = ["any_id"]
    action_col = ["_action"]
    timestamp_col = ["update_time"]
    ids = compound_key + action_col + timestamp_col

    ## Remove compound_key, action and timestamp columns from SAMPLE_SCHEMA
    value_columns = SAMPLE_SCHEMA.fieldNames().copy()
    for c in ids:
        if c in value_columns:
            value_columns.remove(c)
    ## Transfor to long-format table by unpivot
    input = input.unpivot(
        ids=ids, values=value_columns, variableColumnName="col_name", valueColumnName="col_value"
    ).to(CHANGE_LOG_SCHEMA).where(col("col_value").isNotNull())

    # input.writeStream.format("console").option("truncate", "false").start().awaitTermination() ## DEBUGGING

    query = (
        input.writeStream
        .queryName(APP_NAME)
        .trigger(processingTime="3 seconds")
        .foreachBatch(batch_function)
        .option("mergeSchema", "true")
        .option("fanout-enabled", "true")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
