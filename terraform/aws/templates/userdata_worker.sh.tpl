#!/bin/bash

# These commands run when the worker instance is initialized

# exit on failure
set -eo pipefail

tee -a /home/ubuntu/.ssh/config<< END
Host *
  StrictHostKeyChecking no
END

## user defined public key (can optionally be provided by user during cluster creation)
echo '${SSH_PUBLIC_KEY}' > /home/ubuntu/.ssh/id_rsa.pub
echo '${SSH_PUBLIC_KEY}' > /home/ubuntu/.ssh/authorized_keys
echo '${SSH_PRIVATE_KEY}' > /home/ubuntu/.ssh/id_rsa


chown -R ubuntu:ubuntu /home/ubuntu
chmod -R 600 /home/ubuntu/.ssh/
chmod 700 /home/ubuntu/.ssh

sudo -u ubuntu /opt/conda/bin/ipython profile create --profile=default --profile-dir=/home/ubuntu/.ipython/profile_default
sudo chown ubuntu:ubuntu -R /home/ubuntu/.ipython
