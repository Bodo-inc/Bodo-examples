# Spin up a cluster on AWS for Bodo using Terraform

1. [Install Terraform](https://www.terraform.io/downloads.html).

1. Create an AMI. See [this](./bodo-ami/README.md) for instructions. Skip this step if using an existing image.

1. Set up your AWS credentials. We recommend doing this through [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html), but other methods should also work.

1. We recommend using a script like [``bodo_cluster.sh``](./bodo_cluster.sh) to define variables and run terraform.

1. The provided terraform module takes the following variables:

    a. ``AMI_ID`` (required): See step 1.
    
    b. ``VPC_ID`` (required): ID of the VPC you want to deploy the cluster in. You can find this value in your AWS VPC Console. Each AWS account comes with a default VPC.
    
    c. ``CLUSTER_SUBNET_ID`` (required): ID of the subnet to deploy the cluster in. You can find this value in your AWS VPC Console. Ensure that the subnet is inside the VPC defined by ``VPC_ID``.
    
    d. ``USER_IP`` (optional): Your IP address. When provided a security group rule allowing SSH access from this IP to the cluster instances is created. If using [``bodo_cluster.sh``](./bodo_cluster.sh), this is automatically set as the IP of the machine the script is being run on.

    e. ``AWS_DEFAULT_REGION`` (optional): The region to deploy the cluster in. Default: ``us-east-1``.

    f. ``CLUSTER_MEMBERS_COUNT`` (optional): Number of instances in the cluster. Default: 2.

    g. ``CLUSTER_INSTANCE_TYPE`` (optional): Instance type. See [this](https://aws.amazon.com/ec2/instance-types/) for a full list. Default: ``c5n.xlarge``.

    Define the variables in [``bodo_cluster.sh``](./bodo_cluster.sh).

1. Do a dry-run using:

        ./bodo_cluster.sh plan

    This will validate some of the arguments and output a plan of the resources that will be created. If this finishes successfully, move to the next step.

1. Create the cluster using:

        ./bodo_cluster.sh apply

    This will create the cluster and then output the public IPs of the created EC2 instances. 
    The SSH private key for the cluster will be written to ``bodo_cluster_ssh.pem``.
    
1. Log into one of the EC2 instances of the cluster:

        ssh -i bodo_cluster_ssh.pem ubuntu@<PUBLIC_IP_ADDRESS>

    A machinefile for the cluster should already exist at ``/home/ubuntu/machinefile``.
    You can run code using ``mpiexec`` as usual:

        mpiexec -machinefile /home/ubuntu/machinefile -n <TOTAL_CORES_IN_CLUSTER> python ....

1. To delete the cluster, run:

        ./bodo_cluster.sh destroy
