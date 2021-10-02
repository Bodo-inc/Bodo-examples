# Minimal Bodo Docker Image

Bodo.Dockerfile is a minimal Dockerfile that installs [Miniconda](https://docs.conda.io/en/latest/miniconda.html) and Bodo version ``2021.9`` in the base conda environment.

## Build Image with

```
docker build --file Bodo.Dockerfile --tag bodo:latest .
```

## Run using

To open an interactive shell where you can run code using ``mpiexec``, run:

```bash
docker run -it bodo:latest
```

You can also mount this repository to your container using:

```bash
docker run -it -v $(cd ..; pwd):/Bodo-examples  bodo:latest
```
In the interactive shell, you can now run any of the examples in this repository.
For instance, you can run the Monte-Carlo Pi computation example using:

```bash
mpiexec -n 4 python -u /Bodo-examples/examples/miscellaneous/pi.py
```

## Pull image from Dockerhub

This image is also available on Dockerhub in the [bodoaidocker/minimal repository](https://hub.docker.com/r/bodoaidocker/minimal).

Pull the image using:
```bash
docker pull bodoaidocker/minimal
```

# Minimal Bodo Docker Image with JupyterLab

BodoNotebook.Dockerfile is a minimal Dockerfile that builds on top of the popular [jupyter/minimal-notebook:latest](https://hub.docker.com/r/jupyter/minimal-notebook/) Docker image and installs Bodo version ``2021.9`` and IPyParallel.

## Build Image with

```bash
docker build --file BodoNotebook.Dockerfile --tag bodo-notebook:latest .
```

## Run using

To start a JupyterLab server, run:

```bash
docker run -p 8888:8888 bodo-notebook:latest
```

You can also mount this repository to your container using:

```bash
docker run -p 8888:8888 -v $(cd ..; pwd):/home/jovyan/work bodo-notebook:latest
```

See [this notebook](../Bodo+IPyParallel-Starter-Code.ipynb) for an example of how to use IPyParallel to run Bodo code on multiple cores from your notebook.


## Pull image from Dockerhub

This image is also available on Dockerhub in the [bodoaidocker/minimal-jupyter repository](https://hub.docker.com/r/bodoaidocker/minimal-jupyter).

Pull the image using:
```bash
docker pull bodoaidocker/minimal-jupyter
```
