# Running a Bodo workload in Kubernetes

Bodo workloads can be deployed with Kubernetes using the [Kubeflow MPI-Operator](https://github.com/kubeflow/mpi-operator) which enables running MPI applications in a Kubernetes environment. In typical Kubernetes fashion, this also provides resiliency in the case of Node Failure for long running Bodo applications. 

## Prerequisites

- Access to a Kubernetes cluster such as AWS EKS.
- Create a docker image that contains your intended Bodo version and Python scripts. A [pi calculation docker image](https://hub.docker.com/r/bodoaidocker/kube-mpioperator-minimal/tags) is built as an example for testing purposes. To create your own docker images, use the example docker file and python script available at [docker folder](https://github.com/Bodo-inc/Bodo-examples/tree/master/Kubernetes/docker) as the base. 


## Setup
In the following example, we use KOPS to setup Kubernetes Cluster in EKS while this works for any K8s cluster. 

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

- Attempt to create your cluster. This creates a cluster of 2 nodes each with 4 cores. To change the number of instances, modify the `node-count` argument and to change the worker nodes update `node-size`. `master-size` refers to the leader that manages K8s but doesn’t do any computation, so you should keep the instance small. If you are in a different region, change `zones` argument. 
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

- You may use the `example-mpijob.yaml` as it is for the sake of this example. If you wish to modify it with your experiment configuration; update `spec.slotsPerWorker` with the number of physical cores (not vcpus) on each Node and set `spec.mpiReplicaSpecs.Worker.replicas` with the number of worker nodes in your cluster. At last, make sure `-n` is equal to `spec.mpiReplicaSpecs.Worker.replicas` multiplied by `spec.slotsPerWorker`.

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
export KOPS_STATE_STORE=s3://<your S3 bucket name>
kops delete cluster --name $KOPS_CLUSTER_NAME --yes
```
