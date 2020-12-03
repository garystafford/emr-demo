#!/usr/bin/env python3

# Process raw CSV data and output Parquet
# Author: Gary A. Stafford (November 2020)

import argparse

from pyspark.sql import SparkSession


def main():
    args = parse_args()

    spark = SparkSession \
        .builder \
        .appName("bakery-csv-to-parquet") \
        .getOrCreate()

    convert_to_parquet(spark, "bakery", args)


def convert_to_parquet(spark, file, args):
    df_bakery = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(f"s3a://{args.bronze_bucket}/bakery/{file}.csv")

    df_bakery.write \
        .format("parquet") \
        .save(f"s3a://{args.silver_bucket}/bakery/", mode="overwrite")


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--bronze-bucket", required=True, help="Raw data location")
    parser.add_argument("--silver-bucket", required=True, help="Processed data location")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
