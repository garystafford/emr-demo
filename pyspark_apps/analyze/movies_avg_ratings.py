#!/usr/bin/env python3

# Analyze dataset and output results to CSV and Parquet
# Author: Gary A. Stafford (November 2020)

import argparse

from pyspark.sql import SparkSession


def main():
    args = parse_args()

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
        .save(f"s3a://{args.gold_bucket}/movies/avg_ratings/parquet/", mode="overwrite")

    # write single csv file for use with Excel
    df_ratings.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
        .options(delimiter='|') \
        .save(f"s3a://{args.gold_bucket}/movies/avg_ratings/csv/", mode="overwrite")


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description="Arguments required for script.")
    parser.add_argument("--gold-bucket", required=True, help="Analyzed data location")
    parser.add_argument("--start-date", required=True, help="Start of date and time range")
    parser.add_argument("--end-date", required=True, help="End of date and time range")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
