#!/bin/bash

# exit on failure or undefined variables
set -eo pipefail

export TF_VAR_USER_IP=$(curl -s ifconfig.me)
export TF_VAR_AMI_ID="<ENTER-VALID-AMI-ID>"
export TF_VAR_VPC_ID="<ENTER_VALID-VPC-ID>"
export TF_VAR_CLUSTER_SUBNET_ID="<ENTER-VALID-SUBNET-ID>"


ACTION=${1:-plan}

terraform init

case $ACTION in
    'plan') terraform plan
    ;;
    'apply') terraform apply -auto-approve
    ;;
    'destroy') terraform destroy -auto-approve
    ;;
    *) echo "Unrecognized action provided"
    ;;
esac






