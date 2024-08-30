# Running a Bodo workload in Kubernetes

Bodo workloads can be deployed with Kubernetes using the [Kubeflow MPI-Operator](https://github.com/kubeflow/mpi-operator) which enables running MPI applications in a Kubernetes environment. In typical Kubernetes fashion, this also provides increased resiliency in the case of node failure for long running Bodo applications. 

## Prerequisites

- Access to a Kubernetes cluster such as AWS EKS. To start a new Kubernetes Cluster in EKS, you can follow our guide here: [Create EKS Cluster](#create-eks-cluster-using-kops-optional).

- Create a docker image that contains your intended Bodo version and Python scripts and upload it to a docker registry such as Docker Hub so that Kubernetes can pull it. 
For reference, see this [Dockerfile](Dockerfile).
- We have included a python script called `pi.py` which can be used for validating your setup.

A docker image created from this Dockerfile is also available on DockerHub: [ritwikabodo/bodo-kubernetes]().
You can use this as the base image for your own docker image.
In case of private registries, follow instructions from [here](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).


> [!Warning]
> Make sure to provide the correct **CPU and Memory requests** in the yaml file for your Bodo jobs. If correct values are not provided or the cluster doesn't have sufficient CPU or Memory required for the job, the job will be terminated and worker pods may keep respawning. You can estimate the CPU and Memory requirements by running the job locally on a smaller dataset and extrapolating based on that.


**The examples have been tested using Python 3.12 and Bodo 2024.3**

## Setup
Bodo can be deployed in any Kubernetes cluster. For the purposes of this example, we will be using a Kubernetes Cluster in EKS.

#### Step 1: Install MPIJob Custom Resource Definitions(CRD)

- The most up-to-date installation guide is available at [MPI-Operator Github](https://github.com/kubeflow/mpi-operator). This example was tested using [v0.4.0](https://github.com/kubeflow/mpi-operator/tree/v0.4.0), as shown below:

```
git clone https://github.com/kubeflow/mpi-operator --branch v0.4.0
cd mpi-operator
kubectl apply -f deploy/v2beta1/mpi-operator.yaml
```

You can check whether the MPI Job custom resource is installed via:

```
kubectl get crd
```

The output should include `mpijobs.kubeflow.org` similar to:

```console
NAME                   CREATED AT
mpijobs.kubeflow.org    2024-04-02T19:43:04Z
```

#### Step 2: Run your Bodo application

- Define a kubernetes resource for your Bodo workload, such as the one defined in [`mpijob.yaml`](mpijob.yaml) that runs the [pi example](pi.py). You can modify it based on your cluster configuration: 

1. Update `spec.slotsPerWorker` with the number of physical cores (_not_ vCPUs) on each node
2. Set `spec.mpiReplicaSpecs.Worker.replicas` to the number of worker nodes in your cluster.
3. Build the image using the Dockerfile or use `ritwikabodo/bodo-kubernetes` and replace the image at
`spec.mpiReplicaSpecs.Launcher.template.spec.containers.image` and `spec.mpiReplicaSpecs.Worker.template.spec.containers.image`.
4. Check the container arguments is referring to the python file you have intended to run
     ```shell
      args:
         - mpirun
         - -n
         - "8"
         - python
         - /home/mpiuser/chicago_crimes.py
     ```
- Lastly, make sure `-n` is equal to `spec.mpiReplicaSpecs.Worker.replicas` multiplied by `spec.slotsPerWorker`, i.e. the total number of physical cores on your worker nodes.

- Run the example by deploying it in your cluster with `kubectl create -f mpijob.yaml`. This should add 1 pod to each worker and a launcher pod to your master node. 

- View the generated pods by this deployment with `kubectl get pods`. You may inspect any logs by looking at the individual pod's logs.

#### Step 3: Get the Results

- When the job finishes running, your launcher pod will change its status to completed and any stdout information can be found in the logs of the launcher pod:

```
PODNAME=$(kubectl get pods -o=name)
kubectl logs -f ${PODNAME}
```

### Teardown

- When a job has finished running, you can remove it by running `kubectl delete -f mpijob.yaml`. If you want to delete the MPI-Operator crd, please follow the steps on the [MPI-Operator Github repository](https://github.com/kubeflow/mpi-operator).


## Create EKS Cluster using KOPS (Optional)

- Install KOPS on your local machine:

```
# Mac
brew install kops
# Linux
curl -LO https://github.com/kubernetes/kops/releases/download/$(curl -s https://api.github.com/repos/kubernetes/kops/releases/latest | grep tag_name | cut -d '"' -f 4)/kops-linux-amd64
chmod +x kops-linux-amd64
sudo mv kops-linux-amd64 /usr/local/bin/kops
```

- Create a cluster:

Begin by making a bucket in the S3 to use as your `KOPS_STATE_STORE`.  

```
export KOPS_CLUSTER_NAME=imesh.k8s.local
export KOPS_STATE_STORE=s3://<your S3 bucket name>
```

- Attempt to create your cluster: 

This creates a cluster of 2 nodes each with 4 cores. To change the number of instances, modify the `node-count` argument and to change the worker nodes update `node-size`. `master-size` refers to the leader that manages K8s but doesn’t do any Bodo computation, so you should keep the instance small. You can deploy the cluster in a different AWS region and availability zone by modifying the `zones` argument. 

```
kops create cluster \
--node-count=2 \
--node-size=c5.2xlarge \
--master-size=c5.large \
--zones=us-east-2c \
--name=${KOPS_CLUSTER_NAME}
```

- Finish creating the cluster with the below command. This might take several minutes to finish:

```
kops update cluster --name $KOPS_CLUSTER_NAME --yes --admin
```
- Verify the cluster setup is finished by:

```
kops validate cluster
```
