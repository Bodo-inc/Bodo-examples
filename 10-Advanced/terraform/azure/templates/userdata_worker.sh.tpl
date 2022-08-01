#!/bin/bash

# These commands run when the worker VM is initialized

# exit on failure
set -eo pipefail

tee -a /home/azureuser/.ssh/config<< END
Host *
  StrictHostKeyChecking no
END

echo '${SSH_PUBLIC_KEY}' >> /home/azureuser/.ssh/id_rsa.pub
echo '${SSH_PUBLIC_KEY}' > /home/ubuntu/.ssh/authorized_keys
echo '${SSH_PRIVATE_KEY}' >> /home/azureuser/.ssh/id_rsa


chown -R azureuser:azureuser /home/azureuser
chmod -R 600 /home/azureuser/.ssh/
chmod 700 /home/azureuser/.ssh


# Create machinefile
WORKER_IPS="${WORKER_IPS}"

for NODE in $WORKER_IPS; do
  echo "$NODE" >> /home/azureuser/machinefile
done
