#!/usr/bin/env python3

# Purpose: Submit a variable number of Steps defined in a separate JSON file
# Author:  Gary A. Stafford (November 2020)

import argparse
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

ssm_client = boto3.client('ssm')
emr_client = boto3.client('emr')


def main():
    args = parse_args()
    params = get_parameters()
    steps = get_steps(params, args.job_type)

    add_job_flow_steps(params['cluster_id'], steps)


def add_job_flow_steps(cluster_id, steps):
    """Add Steps to an existing EMR cluster"""

    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=steps
        )

        print(f'Response: {response}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_steps(params, job_type):
    """
    Load EMR Steps from a separate JSON-format file and substitutes tags for SSM parameter values
    """

    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    file = open(f'{dir_path}/job_flow_steps/job_flow_steps_{job_type}.json', 'r')

    steps = json.load(file)
    new_steps = []

    for step in steps:
        step['HadoopJarStep']['Args'] = list(
            map(lambda st: str.replace(st, '{{ work_bucket }}', params['work_bucket']), step['HadoopJarStep']['Args']))
        new_steps.append(step)

    return new_steps


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'work_bucket': ssm_client.get_parameter(Name='/emr_demo/work_bucket')['Parameter']['Value'],
        'cluster_id': ssm_client.get_parameter(Name='/emr_demo/cluster_id')['Parameter']['Value']
    }

    return params


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-t', '--job-type', required=True, choices=['process', 'analyze'],
                        help='process or analysis')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
