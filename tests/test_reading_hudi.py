from moto import mock_aws
from pyspark.sql import Row, SparkSession


@mock_aws
def test_reading_hudi_file(spark: SparkSession, test_s3_bucket_name: str):
    expected_data = [
        Row(
            _hoodie_commit_time="20240915233157215",
            _hoodie_commit_seqno="20240915233157215_12_1",
            _hoodie_record_key="20240915233157215_12_0",
            _hoodie_partition_path="sao_paulo",
            _hoodie_file_name="d8d254ed-225e-45f7-8a82-7a403991d7e1-0_12-130-0_20240915233157215.parquet",
            ts=1695516137016,
            uuid="e3cf430c-889d-4015-bc98-59bdce1e530c",
            rider="rider-F",
            driver="driver-P",
            fare=34.15,
            city="sao_paulo",
        ),
        Row(
            _hoodie_commit_time="20240915233157215",
            _hoodie_commit_seqno="20240915233157215_15_5",
            _hoodie_record_key="20240915233157215_15_0",
            _hoodie_partition_path="chennai",
            _hoodie_file_name="3e372d0b-d9a7-4111-8d97-e01522de9747-0_15-133-0_20240915233157215.parquet",
            ts=1695115999911,
            uuid="c8abbe79-8d89-47ea-b4ce-4d224bae5bfa",
            rider="rider-J",
            driver="driver-T",
            fare=17.85,
            city="chennai",
        ),
        Row(
            _hoodie_commit_time="20240915233157215",
            _hoodie_commit_seqno="20240915233157215_3_2",
            _hoodie_record_key="20240915233157215_3_0",
            _hoodie_partition_path="san_francisco",
            _hoodie_file_name="9db15bcf-5d02-46b4-bfe7-fab9de7d98cb-0_3-121-0_20240915233157215.parquet",
            ts=1695159649087,
            uuid="334e26e9-8355-45cc-97c6-c31daf0df330",
            rider="rider-A",
            driver="driver-K",
            fare=19.1,
            city="san_francisco",
        ),
        Row(
            _hoodie_commit_time="20240915233157215",
            _hoodie_commit_seqno="20240915233157215_6_3",
            _hoodie_record_key="20240915233157215_6_0",
            _hoodie_partition_path="san_francisco",
            _hoodie_file_name="f1a8d229-fe36-42f8-a85c-459c2c96d949-0_6-124-0_20240915233157215.parquet",
            ts=1695091554788,
            uuid="e96c4396-3fad-413a-a942-4cb36106d721",
            rider="rider-C",
            driver="driver-M",
            fare=27.7,
            city="san_francisco",
        ),
        Row(
            _hoodie_commit_time="20240915233157215",
            _hoodie_commit_seqno="20240915233157215_9_4",
            _hoodie_record_key="20240915233157215_9_0",
            _hoodie_partition_path="san_francisco",
            _hoodie_file_name="708c0768-47e3-417d-9156-d3456b7d3131-0_9-127-0_20240915233157215.parquet",
            ts=1695046462179,
            uuid="9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            rider="rider-D",
            driver="driver-L",
            fare=33.9,
            city="san_francisco",
        ),
    ]
    actual_result = spark.read.format("hudi").load("s3://test-bucket/simple.hudi")
    assert (
        sorted(actual_result.collect(), key=lambda r: r["_hoodie_commit_seqno"])
        == expected_data
    )
