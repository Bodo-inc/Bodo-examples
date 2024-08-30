FROM condaforge/mambaforge as build

RUN conda install -y python=3.12 bodo=2024.3 mpich=4.1.2 -c bodo.ai -c conda-forge

FROM mpioperator/intel:0.4.0

COPY --from=build /opt/conda /opt/conda

RUN chown -R mpiuser /opt/conda
ENV PATH /opt/conda/bin:${PATH}
USER mpiuser

ADD pi.py pi.py
