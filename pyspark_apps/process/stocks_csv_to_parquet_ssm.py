#!/usr/bin/env python3

# Process raw CSV data and output Parquet
# Author: Gary A. Stafford (November 2020)

import os
from functools import reduce

import boto3
from ec2_metadata import ec2_metadata
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region

ssm_client = boto3.client('ssm')


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("stocks-csv-to-parquet") \
        .getOrCreate()

    df_stocks_list = []

    files = [
        "AAPL", "AXP", "BA", "CAT", "CSCO", "CVX", "DIS", "DWDP", "GS", "HD", "IBM", "INTC", "JNJ", "JPM",
        "KO", "MCD", "MMM", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "V", "VZ", "WBA", "WMT", "XOM"
    ]
    for file in files:
        df_stocks = convert_to_parquet(spark, file, params)
        df_stocks_list.append(df_stocks)

    # list of DataFrames to a single DataFrame
    df_stocks_all = reduce(DataFrame.union, df_stocks_list)

    write_parquet(df_stocks_all, params)


def convert_to_parquet(spark, file, params):
    df_stocks = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(f"s3a://{params['bronze_bucket']}/stocks/{file}.csv")

    df_stocks = df_stocks.withColumn("symbol", lit(str(file.lower())))

    return df_stocks


def write_parquet(df_stocks_all, params):
    df_stocks_all.write \
        .partitionBy("symbol") \
        .format("parquet") \
        .save(f"s3a://{params['silver_bucket']}/stocks/", mode="overwrite")


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'bronze_bucket': ssm_client.get_parameter(Name='/emr_demo/bronze_bucket')['Parameter']['Value'],
        'silver_bucket': ssm_client.get_parameter(Name='/emr_demo/silver_bucket')['Parameter']['Value']
    }

    return params


if __name__ == "__main__":
    main()
