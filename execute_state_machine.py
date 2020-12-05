#!/usr/bin/env python3

# Purpose: Execute Step Function state machine
# Author:  Gary A. Stafford (November 2020)
# Usage Example: python3 ./execute_state_machine.py --state-machine EMR-Demo-Analysis

import argparse
import logging
import os
import uuid

import boto3
from botocore.exceptions import ClientError

step_functions_client = boto3.client('stepfunctions')


def main():
    args = parse_args()
    state_machine_arn = get_state_machine_arn(args.state_machine)  # e.g. 'EMR-Demo-Analysis'

    dir_path = os.path.dirname(os.path.realpath(__file__))

    input_params_file = open(f'{dir_path}/step_functions/inputs/{args.inputs_file}', 'r')
    input_params = input_params_file.read()

    start_execution(state_machine_arn, input_params)


def start_execution(state_machine_arn, input_params):
    """Starts execution of the AWS Step Functions state machine"""

    try:
        response = step_functions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=f'e-{str(uuid.uuid4())}',
            input=input_params
        )
        print(f"executionArn: {response['executionArn']}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_state_machine_arn(state_machine_name):
    state_machines = stepfunctions_client.list_state_machines()
    state_machine_arn = (list(filter(lambda arn: arn['name'] == state_machine_name,
                                     state_machines['stateMachines'])))[0]['stateMachineArn']
    return state_machine_arn


def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-s', '--state-machine', required=True,
                        choices=['EMR-Demo-Process', 'EMR-Demo-Analysis'],
                        help='Name of state machine to execute')
    parser.add_argument('-i', '--inputs-file', required=True, help='Name of state machine inputs file')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
