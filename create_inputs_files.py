#!/usr/bin/env python3

# Purpose: Create input files for both state machines using Jinja2
# Author:  Gary A. Stafford (November 2020)

import logging
import os

import jinja2

from parameters import parameters


def main():
    params = parameters.get_parameters()

    templates = [
        'step_function_inputs_process',
        'step_function_inputs_analyze'
    ]

    for template in templates:
        render_template(template, params)


def render_template(template_name, params):
    """ Render inputs file Jinja2 template using SSM Parameter Store parameter values """

    dir_path = os.path.dirname(os.path.realpath(__file__))
    template_dir = f'{dir_path}/step_functions/templates'
    template_loader = jinja2.FileSystemLoader(searchpath=template_dir)
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(f'{template_name}.j2')

    try:
        output = template.render(
            bronze_bucket=params['bronze_bucket'],
            silver_bucket=params['silver_bucket'],
            gold_bucket=params['gold_bucket'],
            work_bucket=params['work_bucket'],
            logs_bucket=params['logs_bucket'],
            glue_db_bucket=params['glue_db_bucket'],
            bootstrap_bucket=params['bootstrap_bucket'],
            ec2_subnet_id=params['ec2_subnet_id'],
            ec2_key_name=params['ec2_key_name'],
            emr_ec2_role=params['emr_ec2_role'],
            emr_role=params['emr_role']
        )

        with open(f'{dir_path}/step_functions/inputs/{template_name}.json', 'w') as f:
            f.write(output)
            print(f"Input file '{dir_path}/step_functions/inputs/{template_name}.json' rendered")
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    main()
