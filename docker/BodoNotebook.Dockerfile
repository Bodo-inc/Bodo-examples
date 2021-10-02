FROM jupyter/minimal-notebook:latest

ENV JUPYTER_ENABLE_LAB=yes
RUN conda install -y bodo=2021.9 ipyparallel -c bodo.ai -c conda-forge
