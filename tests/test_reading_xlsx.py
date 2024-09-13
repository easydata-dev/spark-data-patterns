from moto import mock_aws
from pyspark.sql import Row, SparkSession


@mock_aws
def test_reading_xlsx_file(spark: SparkSession, test_s3_bucket_name: str):
    expected_data = [
        Row(col1="baz", col2="bang", col3="2"),
        Row(col1="foo", col2="bar", col3="1"),
    ]
    actual_result = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .load(f"s3://{test_s3_bucket_name}/test_simple.xlsx")
    )
    assert sorted(actual_result.collect(), key=lambda row: row["col1"]) == expected_data
