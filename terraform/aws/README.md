# Spin up a cluster on AWS for Bodo using Terraform

1.  [Install Terraform](https://www.terraform.io/downloads.html).

1.  Create an AMI. See [here](./bodo-ami/README.md) for instructions. **Skip this step if using an existing image.**

1.  Set up your AWS credentials. We recommend doing this through [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html), but other methods should also work.

1.  The provided terraform configuration takes the following variables:

    a. `AMI_ID` (required): See step 2.

    b. `VPC_ID` (required): ID of the VPC you want to deploy the cluster in. You can find this value in your AWS VPC Console. Each AWS account comes with a default VPC.

    c. `SUBNET_ID` (required): ID of the subnet to deploy the cluster in. You can find this value in your AWS VPC Console. Ensure that the subnet is inside the VPC defined by `VPC_ID`.

    d. `USER_IP` (optional): Your IP address. When provided, a security group rule allowing SSH access from this IP to the notebook instances is created. If using [`bodo_cluster.sh`](./bodo_cluster.sh), this is automatically set as the IP of the machine the script is being run on.

    e. `AWS_DEFAULT_REGION` (optional): The region to deploy the cluster in. Default: `us-east-1`.

    f. `CLUSTER_MEMBERS_COUNT` (optional): Number of instances in the cluster. Default: 2.

    g. `CLUSTER_INSTANCE_TYPE` (optional): Instance type of worker instances. See [this](https://aws.amazon.com/ec2/instance-types/) for a full list. Default: `c5n.xlarge`.

    g. `NOTEBOOK_INSTANCE_TYPE` (optional): Instance type of notebook instance. This instance runs the JupyterLab server, hence it doesn't need to be a large instance. See [here](https://aws.amazon.com/ec2/instance-types/) for a full list. Default: `c5n.xlarge`.

    The provided [`bodo_cluster.sh`](./bodo_cluster.sh) script is a wrapper around some common terraform commands we'll use to manage our deployment. Set the above variables in this script.

1.  Do a dry-run using:

        ./bodo_cluster.sh plan

    This will validate some of the arguments and output a plan of the resources that will be created. If this finishes successfully, move to the next step.

1.  Create the cluster using:

        ./bodo_cluster.sh apply

    This will create the cluster and then output the link to the JupyterLab server, as well as the public IP of the notebook instance.
    The SSH private key for the cluster will be written to `bodo_cluster_ssh.pem`.
    The EC2 instances and other resources that were provisioned should be visible on your [AWS Management console](https://aws.amazon.com/console/).

1.  Navigate to the JupyterLab server using the link in the terraform output. You can now run your code on this cluster from notebooks using IPyParallel.
    A hostfile for the cluster should already exist at `/home/ubuntu/hostfile`.
    Follow the steps in our [documentation](https://docs.bodo.ai/latest/installation_and_setup/ipyparallel/) to get started.

    For instance, run the following in a notebook to start an IPyParallel cluster:

        import ipyparallel as ipp
        c = ipp.Cluster(engines='mpi',
                        n=4,  # Number of engines: Set this to the total number of physical cores in your cluster
                        controller_ip='*',
                        controller_args=["--nodb"])
        c.engine_launcher_class.mpi_args = ["-f", "/home/ubuntu/hostfile"]
        rc = c.start_and_connect_sync()
        view = rc.broadcast_view(block=True)
        view.activate()

    Now you can execute code on this cluster using the `%%px` magic:

        %%px
        import bodo
        import numpy as np
        import time

        @bodo.jit
        def calc_pi(n):
            t1 = time.time()
            x = 2 * np.random.ranf(n) - 1
            y = 2 * np.random.ranf(n) - 1
            pi = 4 * np.sum(x ** 2 + y ** 2 < 1) / n
            print("Execution time:", time.time() - t1, "\nresult:", pi)

        calc_pi(10000000)

1.  For debugging purposes, you can log into the jupyter instance:

        ssh -i bodo_cluster_ssh.pem ubuntu@<NOTEBOOK_IP>

    A hostfile for the cluster should already exist at `/home/ubuntu/hostfile`.
    You can run code using `mpiexec` as usual:

        mpiexec -f /home/ubuntu/hostfile -n <TOTAL_CORES_IN_CLUSTER> python ....

1.  You can view the terraform outputs at any time using:

        terraform output

1.  To delete the cluster, run:

        ./bodo_cluster.sh destroy
