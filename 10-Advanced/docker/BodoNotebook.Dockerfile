FROM jupyter/minimal-notebook:latest

ENV JUPYTER_ENABLE_LAB=yes
RUN conda install -y bodo ipyparallel -c bodo.ai -c conda-forge
