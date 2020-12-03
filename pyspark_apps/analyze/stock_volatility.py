#!/usr/bin/env python3

# Analyze dataset and output results to CSV and Parquet
# Author: Gary A. Stafford (November 2020)

import argparse

import boto3
from pyspark.sql import SparkSession

ssm_client = boto3.client('ssm')


def main():
    args = parse_args()
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("stock-volatility") \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE `emr_demo`;")

    sql = f"""
        SELECT date, ucase(symbol) AS symbol, close,
            format_number(high, 4) AS high, 
            format_number(low, 4) AS low, 
            format_number(((high - low)/close)*100, 2) AS intraday_volatility
        FROM processed_stocks
        WHERE symbol IN ('aapl', 'ibm', 'msft')
            AND date BETWEEN '{args.start_date}' AND '{args.end_date}'
        ORDER BY intraday_volatility DESC
        LIMIT 10;
    """

    df_stocks = spark.sql(sql)

    # write parquet
    df_stocks.write.format("parquet") \
        .save(f"s3a://{params['gold_bucket']}/stocks/stock_volatility/parquet/", mode="overwrite")

    # write single csv file for use with Excel
    df_stocks.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
        .options(delimiter='|') \
        .save(f"s3a://{params['gold_bucket']}/stocks/stock_volatility/csv/", mode="overwrite")


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--start-date", required=True, help="Start of date and time range")
    parser.add_argument("--end-date", required=True, help="End of date and time range")

    args = parser.parse_args()
    return args


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'silver_bucket': ssm_client.get_parameter(Name='/emr_demo/silver_bucket')['Parameter']['Value'],
        'gold_bucket': ssm_client.get_parameter(Name='/emr_demo/gold_bucket')['Parameter']['Value'],
    }

    return params


if __name__ == "__main__":
    main()
