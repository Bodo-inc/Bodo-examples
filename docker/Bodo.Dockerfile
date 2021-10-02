FROM ubuntu:latest

USER root

RUN apt-get update && apt-get install -y wget bzip2 git vim make curl\
	&& wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
	&& chmod +x miniconda.sh\
	&& ./miniconda.sh -b
ENV PATH /root/miniconda3/bin/:${PATH}

RUN conda install -y python=3.9 bodo=2021.9 -c bodo.ai -c conda-forge

# Useful for long running containers
RUN echo "export PATH=/root/miniconda3/bin/:$PATH" >> ~/.bashrc
