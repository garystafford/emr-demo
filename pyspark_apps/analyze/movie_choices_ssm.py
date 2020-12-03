#!/usr/bin/env python3

# Analyze the dataset and output results to CSV and Parquet
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
        .appName("movie-choices") \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE `emr_demo`;")

    sql = """
        WITH movie_picks AS (
            SELECT m.title,
                date(m.release_date) AS release_date,
                m.popularity,
                count(r.movieid) AS ratings_count,
                round(avg(r.rating), 3) AS avg_rating
            FROM processed_movies_metadata AS m
                LEFT JOIN processed_ratings AS r ON m.id = r.movieid
                LEFT JOIN processed_keywords AS k ON m.id = k.id
                LEFT JOIN processed_credits AS c ON m.id = c.id
            WHERE (
                    lower(k.keywords) LIKE '%artificial intelligence%'
                    OR lower(k.keywords) LIKE '%robot%'
                )
                AND (
                    lower(c.cast) LIKE '%will smith%'
                    OR lower(c.cast) LIKE '%arnold schwarzenegger%'
                    OR lower(c.cast) LIKE '%keanu reeves%'
                )
            GROUP BY m.title,
                m.release_date,
                m.popularity
        )
        SELECT title,
            ntile(3) OVER (
                ORDER BY avg_rating DESC
            ) AS rank,
            avg_rating,
            ratings_count,
            popularity,
            release_date
        FROM movie_picks
        WHERE avg_rating > 0
        ORDER BY rank,
            avg_rating DESC,
            ratings_count DESC;
    """

    df_movies = spark.sql(sql)

    # write parquet
    df_movies.write.format("parquet") \
        .save(f"s3a://{params['gold_bucket']}/movies/movie_choices/parquet/", mode="overwrite")

    # write single csv file for use with Excel
    df_movies.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
        .options(delimiter='|') \
        .save(f"s3a://{params['gold_bucket']}/movies/movie_choices/csv/", mode="overwrite")


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'gold_bucket': ssm_client.get_parameter(Name='/emr_demo/gold_bucket')['Parameter']['Value']
    }

    return params


if __name__ == "__main__":
    main()
