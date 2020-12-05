#!/usr/bin/env python3

# Purpose: Create a new EMR cluster and submits a variable
#          number of Steps defined in a separate JSON file
# Author:  Gary A. Stafford (November 2020)

import argparse
import json
import logging

import boto3
from botocore.exceptions import ClientError

from parameters import parameters

emr_client = boto3.client('emr')


def main():
    args = parse_args()

    params = parameters.get_parameters()
    steps = get_steps(params, args.job_type)
    run_job_flow(params, steps)


def run_job_flow(params, steps):
    """Create EMR cluster, run Steps, and then terminate cluster"""

    try:
        response = emr_client.run_job_flow(
            Name='demo-cluster-run-job-flow',
            LogUri=f's3n://{params["logs_bucket"]}',
            ReleaseLabel='emr-6.2.0',
            Instances={
                'InstanceFleets': [
                    {
                        'Name': 'MASTER',
                        'InstanceFleetType': 'MASTER',
                        'TargetSpotCapacity': 1,
                        'InstanceTypeConfigs': [
                            {
                                'InstanceType': 'm5.xlarge',
                            },
                        ]
                    },
                    {
                        'Name': 'CORE',
                        'InstanceFleetType': 'CORE',
                        'TargetSpotCapacity': 2,
                        'InstanceTypeConfigs': [
                            {
                                'InstanceType': 'r5.2xlarge',
                            },
                        ],
                    },
                ],
                'Ec2KeyName': params['ec2_key_name'],
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2SubnetId': params['ec2_subnet_id'],
            },
            Steps=steps,
            BootstrapActions=[
                {
                    'Name': 'string',
                    'ScriptBootstrapAction': {
                        'Path': f's3://{params["bootstrap_bucket"]}/bootstrap_actions.sh',
                    }
                },
            ],
            Applications=[
                {
                    'Name': 'Spark'
                },
            ],
            Configurations=[
                {
                    'Classification': 'spark-hive-site',
                    'Properties': {
                        'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                    }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole=params['emr_ec2_role'],
            ServiceRole=params['emr_role'],
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': 'Development'
                },
                {
                    'Key': 'Name',
                    'Value': 'EMR Demo Project Cluster'
                },
                {
                    'Key': 'Owner',
                    'Value': 'Data Analytics'
                },
            ],
            EbsRootVolumeSize=32,
            StepConcurrencyLevel=5,
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

    f = open(f'job_flow_steps_{job_type}.json', 'r')
    steps = json.load(f)
    new_steps = []

    for step in steps:
        step['HadoopJarStep']['Args'] = list(
            map(lambda st: str.replace(st, '{{ work_bucket }}', params['work_bucket']), step['HadoopJarStep']['Args']))
        new_steps.append(step)

    return new_steps


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-t', '--job-type', required=True, choices=['process', 'analyze'], help='process or analysis')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
