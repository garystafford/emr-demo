#!/bin/bash

# set region for boto3
sudo yum install -y jq

aws configure set region \
  "$(curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)"

# install some useful python packages
sudo python3 -m pip install boto3 ec2-metadata scipy scikit-learn pandas awswrangler