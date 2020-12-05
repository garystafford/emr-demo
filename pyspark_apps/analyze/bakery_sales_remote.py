#!/usr/bin/env python3

# Analyze dataset and output results to CSV and Parquet
# Author: Gary A. Stafford (November 2020)

import argparse

from pyspark.sql import SparkSession


def main():
    spark = SparkSession \
        .builder \
        .appName("bakery-sales") \
        .master("spark://ec2-34-207-136-211.compute-1.amazonaws.com:8443") \
        .getOrCreate()

    df_bakery = spark.read \
        .format("parquet") \
        .load(f"s3a://emr-demo-raw-676164205626-us-east-1/bakery/")

    df_sorted = df_bakery.cube("item").count() \
        .filter("item NOT LIKE 'NONE'") \
        .filter("item NOT LIKE 'Adjustment'") \
        .orderBy(["count", "item"], ascending=[False, True])

    # # write parquet
    # df_sorted.write.format("parquet") \
    #     .save(f"s3a://{args.gold_bucket}/bakery/bakery_sales/parquet/", mode="overwrite")
    #
    # # write single csv file for use with Excel
    # df_sorted.coalesce(1) \
    #     .write.format("csv") \
    #     .option("header", "true") \
    #     .options(delimiter='|') \
    #     .save(f"s3a://{args.gold_bucket}/bakery/bakery_sales/csv/", mode="overwrite")


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--silver-bucket", required=True, help="Processed data location")
    parser.add_argument("--gold-bucket", required=True, help="Analyzed data location")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
