import os
import signal
import subprocess
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession

BUCKET_NAME = "test-bucket"


@pytest.fixture(autouse=True, scope="session")
def moto_endpoint():
    print("Set Up Moto Server")
    process = subprocess.Popen(
        "moto_server", stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid
    )

    yield "http://127.0.0.1:5000"
    print("Tear Down Moto Server")
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)


@pytest.fixture(autouse=True, scope="session")
def spark(moto_endpoint):

    os.environ["SPARK_LOCAL_IP"] = "localhost"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "'
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.crealytics:spark-excel_2.12:3.5.1_0.20.4"
        '" '
        "pyspark-shell"
    )

    spark = SparkSession.builder.config(
        map={
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": "foo",
            "spark.hadoop.fs.s3a.secret.key": "foo",
            "spark.hadoop.fs.s3a.endpoint": moto_endpoint,
        },
    ).getOrCreate()
    yield spark


@pytest.fixture(autouse=True, scope="session")
def test_s3_bucket_name(moto_endpoint) -> str:
    mock_aws().start()
    s3_connection = boto3.resource(
        "s3", region_name="us-east-1", endpoint_url=moto_endpoint
    )
    s3_connection.create_bucket(Bucket=BUCKET_NAME)

    s3_connection = boto3.resource(
        "s3", region_name="us-east-1", endpoint_url=moto_endpoint
    )

    local_directory = Path(__file__).parent / "fixtures"
    bucket = s3_connection.Bucket(BUCKET_NAME)

    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = relative_path.replace("\\", "/")
            bucket.upload_file(local_path, s3_path)
    mock_aws().stop()
    yield BUCKET_NAME
