#!/usr/bin/env python3

# Purpose: Submit Spark job to EMR Master Node
# Author:  Gary A. Stafford (December 2020)
# Usage Example: python3 ./submit_spark_ssh.py \
#                    --ec2-key-path ~/.ssh/emr-demo-123456789012-us-east-1.pem

import argparse
import logging

import boto3
from paramiko import SSHClient, AutoAddPolicy

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

ssm_client = boto3.client('ssm')


def main():
    args = parse_args()
    params = get_parameters()
    submit_job(params['master_public_dns'], 'hadoop', args.ec2_key_path, params['work_bucket'])


def submit_job(master_public_dns, username, ec2_key_path, work_bucket):
    """Submit job to EMR Master Node"""

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    ssh.connect(hostname=master_public_dns, username=username, key_filename=ec2_key_path)

    stdin_, stdout_, stderr_ = ssh.exec_command(
        command=f"""
            spark-submit --deploy-mode cluster --master yarn \
            --conf spark.yarn.submit.waitAppCompletion=true \
            s3a://{work_bucket}/analyze/bakery_sales_ssm.py"""
    )

    stdout_lines = ''
    while not stdout_.channel.exit_status_ready():
        if stdout_.channel.recv_ready():
            stdout_lines = stdout_.readlines()
    logging.info(' '.join(map(str, stdout_lines)))

    ssh.close()


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'master_public_dns': ssm_client.get_parameter(Name='/emr_demo/master_public_dns')['Parameter']['Value'],
        'work_bucket': ssm_client.get_parameter(Name='/emr_demo/work_bucket')['Parameter']['Value']
    }

    return params


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-e', '--ec2-key-path', required=True, help='EC2 Key Path')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
