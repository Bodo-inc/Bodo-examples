FROM condaforge/mambaforge as build
ENV DEBIAN_FRONTEND noninteractive

ENV JUPYTER_ENABLE_LAB=yes

RUN mamba install bodosql=2023.12.1 -c bodo.ai/label/dev -c conda-forge
RUN mamba install -y ipyparallel=8.1 jupyterlab=3 -c conda-forge

#RUN conda remove --force --yes mpich mpi
RUN mamba clean -a -y


FROM ubuntu:latest

USER root
COPY --chown=root --from=build /opt/conda /opt/conda

# Install Python and necessary packages
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget bzip2 git vim make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Create a working directory for the Jupyter Notebook
WORKDIR /app

# Copy the two Python files to the container
COPY pi.ipynb quickstart_notebook.ipynb /app/

# Expose port 8888 for the Jupyter Notebook
EXPOSE 8888

ENV PATH=/opt/conda/bin:$PATH

# Start IPyParallel cluster with max 8 cores
CMD ["sh", "-c", "ipcluster start --profile=default --n=$(($(nproc)-1)) --nthreads=$(($(nproc)-1)) --engines=$(($(nproc)>8 ? 8 : $(nproc)-1)) & jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root"]