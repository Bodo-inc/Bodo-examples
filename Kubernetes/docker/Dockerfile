FROM mpioperator/intel:latest

WORKDIR /home/mpiuser

# Install Miniconda
RUN apt-get update && apt-get install -y wget bzip2 git vim make curl\
	&& wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
	&& chmod +x miniconda.sh\
	&& ./miniconda.sh -p /home/mpiuser/miniconda3 -b
ENV PATH /home/mpiuser/miniconda3/bin/:${PATH}

RUN conda install -y python=3.9 bodo=2021.11 -c bodo.ai -c conda-forge

RUN conda remove --force --yes mpich mpi

ADD pi.py pi.py
