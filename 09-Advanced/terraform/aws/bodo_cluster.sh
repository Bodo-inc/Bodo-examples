#!/bin/bash

# exit on failure or undefined variables
set -eo pipefail

export TF_VAR_AMI_ID="<ENTER-VALID-AMI-ID>"
export TF_VAR_VPC_ID="<ENTER_VALID-VPC-ID>"
export TF_VAR_SUBNET_ID="<ENTER-VALID-SUBNET-ID>"
export TF_VAR_USER_IP=$(curl -s ifconfig.me)

## Optional environment variables to specify cluster configuration (instance type, number of instances, etc.). 
## See default values in variables.tf.

# export TF_VAR_AWS_DEFAULT_REGION=
# export TF_VAR_CLUSTER_MEMBERS_COUNT=
# export TF_VAR_CLUSTER_INSTANCE_TYPE=
# export TF_VAR_NOTEBOOK_INSTANCE_TYPE=


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
