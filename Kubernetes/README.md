# Running a Bodo workload in Kubernetes

Bodo workloads can be deployed with Kubernetes using the [Kubeflow MPI-Operator](https://github.com/kubeflow/mpi-operator) which enables running MPI applications in a Kubernetes environment. In typical Kubernetes fashion, this also provides increased resiliency in the case of Node Failure for long running Bodo applications. 

## Prerequisites

- Access to a Kubernetes cluster such as AWS EKS.

- Create a docker image that contains your intended Bodo version and Python scripts and upload it to a docker registry such as Docker Hub so that K8s can pull it. For reference, see this [Dockerfile](docker/Dockerfile).
In case of private registries, follow instructions from [here](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).
A docker image created from this Dockerfile is also available on DockerHub: [bodoaidocker/kube-mpioperator-minimal](https://hub.docker.com/r/bodoaidocker/kube-mpioperator-minimal/tags). 
You can use this as the base image for your own docker image. For testing and validation purposes this image also includes the [pi calculation example](docker/pi.py), which is used in this tutorial.


## Setup
Bodo can be deployed in any Kubernetes cluster. For the purposes of this example, we set up a Kubernetes Cluster in EKS using KOPS:

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

Begin by making a bucket in the S3 to use as your `KOPS_STATE_STORE`.  

```
export KOPS_CLUSTER_NAME=imesh.k8s.local
export KOPS_STATE_STORE=s3://<your S3 bucket name>
```

- Attempt to create your cluster: 

This creates a cluster of 2 nodes each with 4 cores. To change the number of instances, modify the `node-count` argument and to change the worker nodes update `node-size`. `master-size` refers to the leader that manages K8s but doesn’t do any computation, so you should keep the instance small. You can deploy the cluster in a different AWS region and availability zone by modifying the `zones` argument. 

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

- Define a kubernetes resource for your Bodo workload, such as the one defined in [`example-mpijob.yaml`](example-mpijob.yaml) that runs the [pi calculation example](docker/pi.py). You can modify it based on your cluster configuration: update `spec.slotsPerWorker` with the number of physical cores (_not_ vCPUs) on each node and set `spec.mpiReplicaSpecs.Worker.replicas` to the number of worker nodes in your cluster. Lastly, make sure `-n` is equal to `spec.mpiReplicaSpecs.Worker.replicas` multiplied by `spec.slotsPerWorker`, i.e. the total number of physical cores on your worker nodes. If you're using the cluster configuration as defined in step 1, you do not need to modify anything.

- Run the example by deploying it in your cluster with `kubectl create -f example-mpijob.yaml`. This should add 1 pod to each worker and a launcher pod to your master node. 

- View the generated pods by this deployment with `kubectl get pods`. You may inspect any logs by looking at the individual pod's logs.

### Step 4: Get the Results

- When the job finishes running, your launcher pod will change its status to completed and any stdout information can be found in the logs of the launcher pod:

```
PODNAME=$(kubectl get pods -o=name)
kubectl logs -f ${PODNAME}

```

## Teardown

- When a job has finished running, you can remove it by running `kubectl delete -f example-mpijob.yaml`. If you want to delete the MPI-Operator crd, please follow the steps on the [MPI-Operator Github repository](https://github.com/kubeflow/mpi-operator).

- Tear down your cluster with the following script:
```
export KOPS_CLUSTER_NAME=imesh.k8s.local
export KOPS_STATE_STORE=s3://<your S3 bucket name>
kops delete cluster --name $KOPS_CLUSTER_NAME --yes
```
