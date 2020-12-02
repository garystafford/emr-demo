#!/usr/bin/env python3

# Delete EMR Demo buckets
# Author: Gary A. Stafford (November 2020)

import logging

import boto3
from botocore.exceptions import ClientError
from parameters import parameters

s3_client = boto3.resource('s3')


def main():
    buckets = parameters.get_parameters()

    delete_buckets(buckets)


def delete_buckets(buckets):
    for bucket in buckets.values():
        try:
            print(bucket)
            bucket_to_delete = s3_client.Bucket(bucket)
            bucket_to_delete.object_versions.delete()
            bucket_to_delete.delete()
            print(f"Bucket deleted: {bucket}")
        except ClientError as e:
            logging.error(e)


if __name__ == '__main__':
    main()
