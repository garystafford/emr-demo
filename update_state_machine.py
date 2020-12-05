#!/usr/bin/env python3

# Purpose: Create Step Function state machine
# Author:  Gary A. Stafford (November 2020)
# Usage Example: python3 ./update_state_machine.py \
#                   --definition-file step_function_emr_analyze.json \
#                   --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:EMR-Demo-Analysis

import argparse
import logging
import os

import boto3
from botocore.exceptions import ClientError

step_functions_client = boto3.client('stepfunctions')
ssm_client = boto3.client('ssm')


def main():
    args = parse_args()
    params = get_parameters()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    definition_file = open(f'{dir_path}/step_functions/definitions/{args.definition_file}', 'r')
    definition = definition_file.read()

    create_state_machine(definition, args.state_machine_arn, params['sm_log_group_arn'], params['sm_role_arn'])


def create_state_machine(definition, state_machine_arn, sm_log_group_arn, sm_role_arn):
    """Updates an existing AWS Step Functions state machine"""

    try:
        response = step_functions_client.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=definition,
            roleArn=sm_role_arn,
            loggingConfiguration={
                'level': 'ERROR',
                'includeExecutionData': False,
                'destinations': [
                    {
                        'cloudWatchLogsLogGroup': {
                            'logGroupArn': sm_log_group_arn
                        }
                    },
                ]
            }
        )
        print(f"Response: {response}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-d', '--definition-file', required=True, help='Name of definition file')
    parser.add_argument('-a', '--state-machine-arn', required=True, help='Arn of state machine to update')
    args = parser.parse_args()
    return args


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'sm_role_arn': ssm_client.get_parameter(Name='/emr_demo/sm_role_arn')['Parameter']['Value'],
        'sm_log_group_arn': ssm_client.get_parameter(Name='/emr_demo/sm_log_group_arn')['Parameter']['Value']
    }

    return params


if __name__ == '__main__':
    main()
