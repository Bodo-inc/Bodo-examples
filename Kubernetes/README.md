# Bodo Kubernetes Example

This folder contains information about how to deploy a Bodo application with Kubernetes. We deploy Bodo with the [Kubeflow MPI-Operator](https://github.com/kubeflow/mpi-operator), which enables resiliency in the case of Node Failure for long running Bodo applications. 

## Prerequisites

- <strong>Docker image </strong>: Create a docker image that contains your intended Bodo version and Python scripts. For pi calculation, you may use [bodoaidocker/pi:latest](https://hub.docker.com/r/bodoaidocker/pi/tags). 
- <strong>AWS </strong>: Have Access-Key/Secret-Key pair.


## Setup

### Step 1: Create a Kubernetes Cluster in EKS

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

To change the number of instances, modify the `node-count` argument and to change the worker nodes update `node-size`. `master-size` refers to the leader that manages K8s but doesn’t do any computation, so you should keep the instance small. If you are in a different region, change `zones` argument. 

```
export KOPS_CLUSTER_NAME=imesh.k8s.local
export KOPS_STATE_STORE=s3://mpijob-cluster-bucket
```

- Attempt to create your cluster. This creates a cluster of 2 nodes each with 4 cores
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

### Step 2: Install MPIJob

- The most up-to-date installation guide is available at [MPI-Operator Github](https://github.com/kubeflow/mpi-operator). You may also follow the below steps :

```
git clone https://github.com/kubeflow/mpi-operator
cd mpi-operator
kubectl apply -f deploy/v2beta1/mpi-operator.yaml
```

You can check whether the MPI Job custom resource is installed via:

```
kubectl get crd
```

The output should include mpijobs.kubeflow.org like the following:

```
NAME                   CREATED AT
mpijobs.kubeflow.org   2022-01-03T21:19:10Z
```

### Step 3: Run your Bodo application

- You may use the `example-mpijob.yaml` as it is for the sake of this example. If you wish to modify it with your experiment configuration; update `spec.slotsPerWorker` with the number of physical cores (not vcpus) on each Node and set `spec.mpiReplicaSpecs.Worker.replicas` with the number of workers node in your cluster. At last, make sure `-n` matches your cluster.

- Run the example by deploying it over your cluster with `kubectl create -f example-mpijob.yaml`. This should add 1 pod to each worker and a launcher pod to your master node. 

- View the generated pods by this deployment with `kubectl get pods`. You may inspect any logs by looking at the individual pod's logs.

### Step 4: Get the Results

- When the job finishes running, your launcher pod will change its status to completed and any stdout information can be found in the logs of the launcher pod:

```
PODNAME=$(kubectl get pods -o=name)
kubectl logs -f ${PODNAME}

```

## Teardown

- When a job has finished running, you can remove it by running `kubectl delete -f example-mpijob.yaml`. If you want to delete the MPI-Operator crd, please follow any steps on the [MPI-Operator Github](https://github.com/kubeflow/mpi-operator).

- Tear down your cluster with the following script:
```
export KOPS_CLUSTER_NAME=imesh.k8s.local
export KOPS_STATE_STORE=s3://mpijob-cluster-bucket
kops delete cluster --name $KOPS_CLUSTER_NAME --yes
```
