#!/usr/bin/env python3

# Start a AWS Glue Crawler
# Author: Gary A. Stafford (November 2020)

import argparse
import logging

import boto3
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')


def main():
    args = parse_args()

    start_crawler(args.crawler_name)


def start_crawler(crawler_name):
    """Start the specified AWS Glue Crawler"""

    try:
        response = glue_client.start_crawler(
            Name=crawler_name
        )
        print(f'Response: {response}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-c', '--crawler-name', required=True, help='Name of EC2 Keypair')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
