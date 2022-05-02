FROM mpioperator/openmpi:latest

WORKDIR /home/mpiuser

SHELL ["/bin/bash", "-cu"]

RUN apt-get update && apt-get install -y wget bzip2 git vim make curl\
	&& wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
	&& chmod +x miniconda.sh\
	&& ./miniconda.sh -b
ENV PATH /root/miniconda3/bin/:${PATH}

RUN conda install -y python=3.9 -c conda-forge

RUN pip install bodo

ADD Kubernetes/docker/chicago_crimes.py .