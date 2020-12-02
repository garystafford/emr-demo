#!/usr/bin/env python3

# Purpose: Create EMR bootstrap script(s) bucket
# Author:  Gary A. Stafford (November 2020)
# Reference: https://gist.github.com/svrist/73e2d6175104f7ab4d201280acba049c
# Usage Example: python3 ./create_cfn_stack.py --ec2-key-name emr-demo-123456789012-us-east-1

import argparse
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

sts_client = boto3.client('sts')
ssm_client = boto3.client('ssm')
cfn_client = boto3.client('cloudformation')
region = boto3.DEFAULT_SESSION.region_name
s3_client = boto3.client('s3', region_name=region)


def main():
    args = parse_args()

    # create bucket
    account_id = sts_client.get_caller_identity()['Account']
    bucket_name = f'emr-demo-bootstrap-{account_id}-{region}'
    create_bucket(bucket_name)
    put_parameter(bucket_name)

    # upload files
    dir_path = os.path.dirname(os.path.realpath(__file__))

    # files = [
    #     {'path': f'{dir_path}/cloudformation/emr-demo.yml', 'name': 'emr-demo.yml'},
    #     {'path': f'{dir_path}/cloudformation/emr-demo-params-dev.json', 'name': 'emr-demo-params-dev.json'},
    #     {'path': f'{dir_path}/scripts/bootstrap_emr/bootstrap_actions.sh', 'name': 'bootstrap_actions.sh'}
    # ]

    upload_file(f'{dir_path}/bootstrap_emr/bootstrap_actions.sh', bucket_name, 'bootstrap_actions.sh')

    # create stack
    stack_name = 'emr-demo-dev'
    cfn_template_path = f'{dir_path}/cloudformation/emr-demo.yml'
    cfn_params_path = f'{dir_path}/cloudformation/emr-demo-params-dev.json'
    ec2_key_name = args.ec2_key_name
    create_stack(stack_name, cfn_template_path, cfn_params_path, ec2_key_name, bucket_name)


def create_bucket(bucket_name):
    """Create an S3 bucket in a specified region

    :param bucket_name: Bucket to create
    :return: True if bucket created, else False
    """

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f' New bucket name: {bucket_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name
    :return: True if file was uploaded, else False
    """

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print(f'File {file_name} uploaded to bucket {bucket} as object {object_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def put_parameter(bucket_name):
    try:
        response = ssm_client.put_parameter(
            Name='/emr_demo/bootstrap_bucket',
            Description='Bootstrap scripts bucket',
            Value=bucket_name,
            Type='String',
            # Overwrite=True,
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': 'Development'
                },
            ]
        )
        print(f' Response: {response}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def create_stack(stack_name, cfn_template, cfn_params, ec2_key_name, bucket_name):
    template_data = _parse_template(cfn_template)
    parameter_data = _parse_parameters(cfn_params)

    # substitute static placeholder values for dynamic parameter values
    for d in parameter_data:
        d.update((k, ec2_key_name) for k, v in d.items() if v == 'placeholder-ec2-key-name')
        d.update((k, bucket_name) for k, v in d.items() if v == 'placeholder-bootstrap-bucket')

    create_stack_params = {
        'StackName': stack_name,
        'TemplateBody': template_data,
        'Parameters': parameter_data,
        'TimeoutInMinutes': 60,
        'Capabilities': [
            'CAPABILITY_NAMED_IAM',
        ],
        'Tags': [
            {
                'Key': 'Project',
                'Value': 'EMR Demo'
            },
        ]
    }

    try:
        response = cfn_client.create_stack(**create_stack_params)
        print(f' Response: {response}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def _parse_template(template):
    with open(template) as template_file_obj:
        template_data = template_file_obj.read()
    cfn_client.validate_template(TemplateBody=template_data)
    return template_data


def _parse_parameters(parameters):
    with open(parameters) as parameter_file_obj:
        parameter_data = json.load(parameter_file_obj)
    return parameter_data


def parse_args():
    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-k', '--ec2-key-name', required=True, help='Name of EC2 Keypair')
    parser.add_argument('-a', '--action', required=False, default='create',
                        help='CloudFormation stack action', choices=['create', 'update'])

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
