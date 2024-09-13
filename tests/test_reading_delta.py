from moto import mock_aws
from pyspark.sql import Row, SparkSession


@mock_aws
def test_reading_delta_file(spark: SparkSession, test_s3_bucket_name: str):
    expected_data = [
        Row(col1="banana", col2="apple"),
        Row(col1="plum", col2="orange"),
    ]

    actual_result = spark.read.format("delta").load(
        f"s3://{test_s3_bucket_name}/simple.delta"
    )
    assert sorted(actual_result.collect(), key=lambda row: row["col1"]) == expected_data
