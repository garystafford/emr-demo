#!/usr/bin/env python3

# Analyze the dataset and output results to CSV and Parquet
# Author: Gary A. Stafford (November 2020)

import argparse
import os

import boto3
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region

ssm_client = boto3.client('ssm')


def main():
    args = parse_args()
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("movie-ratings") \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE `emr_demo`;")

    sql = f"""
        SELECT ntile(10) OVER (ORDER BY r.rating DESC) AS rank,
            avg(r.rating) AS avg_rating,
            count(r.movieid) AS ratings_count,
            round(avg(cast(m.popularity AS DOUBLE)), 2) AS popularity
        FROM processed_movies_metadata AS m
            LEFT JOIN processed_ratings_small AS r ON m.id = r.movieid
        WHERE r.rating IS NOT NULL
            AND from_unixtime(r.timestamp) >= to_timestamp('{args.start_date}')
            AND from_unixtime(r.timestamp) <= to_timestamp('{args.end_date}')
        GROUP BY r.rating;
    """

    df_ratings = spark.sql(sql)

    # write parquet
    df_ratings.write.format("parquet") \
        .save(f"s3a://{params['gold_bucket']}/movies/avg_ratings/parquet/", mode="overwrite")

    # write single csv file for use with Excel
    df_ratings.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
        .options(delimiter='|') \
        .save(f"s3a://{params['gold_bucket']}/movies/avg_ratings/csv/", mode="overwrite")


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
        'gold_bucket': ssm_client.get_parameter(Name='/emr_demo/gold_bucket')['Parameter']['Value']
    }

    return params


if __name__ == "__main__":
    main()
