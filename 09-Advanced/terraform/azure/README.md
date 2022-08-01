# Spin up a cluster on Azure for Bodo using Terraform

1. [Install Terraform](https://www.terraform.io/downloads.html).

1. Create a VMI. See [this](./bodo-vmi/README.md) for instructions. Skip this step if using an existing image.

1. [Install Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) if it's not already installed. 

1. Login using Azure CLI:

        az login

1. The provided terraform configuration takes the following variables:

    a. ``AZ_SUBSCRIPTION_ID`` (required): Subscription ID of your Azure Subscription.
    
    b. ``IMAGE_ID`` (required): See step 2.
    
    c. ``USER_IP`` (optional): Your IP address. When provided a security group rule allowing SSH access from this IP to the cluster instances is created. If using [``bodo_cluster.sh``](./bodo_cluster.sh), this is automatically set as the IP of the machine the script is being run on.

    d. ``LOCATION`` (optional): The region to deploy the cluster in. Default: ``eastus``.

    e. ``CLUSTER_MEMBERS_COUNT`` (optional): Number of instances in the cluster. Default: 2.

    f. ``CLUSTER_INSTANCE_TYPE`` (optional): Instance type. See [this](https://docs.microsoft.com/en-us/azure/virtual-machines/sizes) for a full list. Default: ``Standard_D4as_v4``.

    g. ``ENABLE_ACCELERATED_NETWORKING`` (optional): Type of NIC available on certain instance types. Read more about it [here](https://docs.microsoft.com/en-us/azure/virtual-network/create-vm-accelerated-networking-cli). ``Standard_D4as_v4`` does support it.  Default: ``true``.

    The provided [``bodo_cluster.sh``](./bodo_cluster.sh) script is a wrapper around some common terraform commands we'll use to manage our deployment. Set the above variables in this script.

1. Do a dry-run using:

        ./bodo_cluster.sh plan

    This will validate some of the arguments and output a plan of the resources that will be created. If this finishes successfully, move to the next step.

1. Create the cluster using:

        ./bodo_cluster.sh apply

    This will create the cluster and then output the public IPs of the created VMs. 
    The SSH private key for the cluster will be written to ``bodo_cluster_ssh.pem``.
    The VMs and other resources that were provisioned should be visible on your [Azure Portal](https://portal.azure.com/).
    
1. Log into one of the VMs of the cluster:

        ssh -i bodo_cluster_ssh.pem azureuser@<PUBLIC_IP_ADDRESS>

    A machinefile for the cluster should already exist at ``/home/azureuser/machinefile``.
    You can run code using ``mpiexec`` as usual:

        mpiexec -machinefile /home/azureuser/machinefile -n <TOTAL_CORES_IN_CLUSTER> python ....

1. To delete the cluster, run:

        ./bodo_cluster.sh destroy
