from moto import mock_aws
from pyspark.sql import Row, SparkSession


@mock_aws
def test_spark(spark: SparkSession, test_s3_bucket_name: str):

    expected_data = [
        Row(name="Alice", age=30),
        Row(name="Bob", age=25),
        Row(name="Cathy", age=27),
    ]
    actual_result = spark.read.parquet(
        f"s3://{test_s3_bucket_name}/simple_data.parquet"
    )
    assert sorted(actual_result.collect(), key=lambda row: row["name"]) == expected_data
