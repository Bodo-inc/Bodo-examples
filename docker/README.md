# Bodo Docker Image

Bodo.Dockerfile is a minimal Dockerfile that installs [Miniconda](https://docs.conda.io/en/latest/miniconda.html) and installs Bodo version ``2021.9`` in the base conda environment.

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
