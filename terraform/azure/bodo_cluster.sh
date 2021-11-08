#!/bin/bash

# exit on failure or undefined variables
set -eo pipefail

export TF_VAR_AZ_SUBSCRIPTION_ID="<ENTER-VALID-SUBSCRIPTION-ID>"
export TF_VAR_IMAGE_ID="<ENTER-VALID-IMAGE-ID>"
export TF_VAR_USER_IP=$(curl -s ifconfig.me)

## Optional environment variables to specify cluster configuration (instance type, number of instances, etc.). 
## See default values in variables.tf.

# export TF_VAR_LOCATION=
# export TF_VAR_CLUSTER_MEMBERS_COUNT=
# export TF_VAR_CLUSTER_INSTANCE_TYPE=
# export TF_VAR_ENABLE_ACCELERATED_NETWORKING=


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
