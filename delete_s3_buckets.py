#!/usr/bin/env python3

# Delete EMR Demo buckets
# Author: Gary A. Stafford (November 2020)

import logging

import boto3
from botocore.exceptions import ClientError

s3_client = boto3.resource('s3')
ssm_client = boto3.client('ssm')


def main():
    buckets = get_parameters()

    delete_buckets(buckets)


def delete_buckets(buckets):
    """ Delete all Amazon S3 buckets created for this project """

    for bucket in buckets.values():
        try:
            bucket_to_delete = s3_client.Bucket(bucket)
            bucket_to_delete.object_versions.delete()
            bucket_to_delete.delete()
            print(f"Bucket deleted: {bucket}")
        except ClientError as e:
            logging.error(e)


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'bootstrap_bucket': ssm_client.get_parameter(Name='/emr_demo/bootstrap_bucket')['Parameter']['Value'],
        'bronze_bucket': ssm_client.get_parameter(Name='/emr_demo/bronze_bucket')['Parameter']['Value'],
        'glue_db_bucket': ssm_client.get_parameter(Name='/emr_demo/glue_db_bucket')['Parameter']['Value'],
        'gold_bucket': ssm_client.get_parameter(Name='/emr_demo/gold_bucket')['Parameter']['Value'],
        'logs_bucket': ssm_client.get_parameter(Name='/emr_demo/logs_bucket')['Parameter']['Value'],
        'silver_bucket': ssm_client.get_parameter(Name='/emr_demo/silver_bucket')['Parameter']['Value'],
        'work_bucket': ssm_client.get_parameter(Name='/emr_demo/work_bucket')['Parameter']['Value']
    }

    return params


if __name__ == '__main__':
    main()
