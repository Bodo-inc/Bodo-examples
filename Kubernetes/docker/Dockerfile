FROM mpioperator/intel:latest

WORKDIR /home/mpiuser

RUN apt-get update && apt-get install -y wget bzip2 git vim make curl\
        && wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
        && chmod +x miniconda.sh\
        && ./miniconda.sh -b
ENV PATH /root/miniconda3/bin/:${PATH}

RUN conda install -y mamba -c conda-forge && mamba install -y bodo -c bodo.ai -c conda-forge

RUN conda remove --force --yes mpich mpi

RUN chown -R mpiuser /root

USER mpiuser

ENV PATH /root/miniconda3/bin/python:${PATH}

ADD chicago_crimes.py chicago_crimes.py