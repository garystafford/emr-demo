#!/usr/bin/env python3

# Process raw CSV data and output Parquet
# Author: Gary A. Stafford (November 2020)

import os

import boto3
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region

ssm_client = boto3.client('ssm')


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("bakery-csv-to-parquet") \
        .getOrCreate()

    convert_to_parquet(spark, "bakery", params)


def convert_to_parquet(spark, file, params):
    df_bakery = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(f"s3a://{params['bronze_bucket']}/bakery/{file}.csv")

    write_parquet(df_bakery, params)


def write_parquet(df_bakery, params):
    df_bakery.write \
        .format("parquet") \
        .save(f"s3a://{params['silver_bucket']}/bakery/", mode="overwrite")


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'bronze_bucket': ssm_client.get_parameter(Name='/emr_demo/bronze_bucket')['Parameter']['Value'],
        'silver_bucket': ssm_client.get_parameter(Name='/emr_demo/silver_bucket')['Parameter']['Value']
    }

    return params


if __name__ == "__main__":
    main()
