#!/usr/bin/env python3

# Process raw CSV data and output Parquet
# Author: Gary A. Stafford (November 2020)

import argparse
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def main():
    args = parse_args()

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
        df_stocks = convert_to_parquet(spark, file, args)
        df_stocks_list.append(df_stocks)

    # list of DataFrames to a single DataFrame
    df_stocks_all = reduce(DataFrame.union, df_stocks_list)

    write_parquet(df_stocks_all, args)


def convert_to_parquet(spark, file, args):
    df_stocks = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(f"s3a://{args.bronze_bucket}/stocks/{file}.csv")

    df_stocks = df_stocks.withColumn("symbol", lit(str(file.lower())))

    return df_stocks


def write_parquet(df_stocks_all, args):
    df_stocks_all.write \
        .partitionBy("symbol") \
        .format("parquet") \
        .save(f"s3a://{args.silver_bucket}/stocks/", mode="overwrite")


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--bronze-bucket", required=True, help="Raw data location")
    parser.add_argument("--silver-bucket", required=True, help="Processed data location")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
