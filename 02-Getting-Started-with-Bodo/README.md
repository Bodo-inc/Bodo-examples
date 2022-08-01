# Bodo Tutorial
Welcome to Bodo Tutorials!

First make sure you have Bodo [installed](https://docs.bodo.ai/installation_and_setup/install/) by following the commands below.
Bodo can be installed using Conda. To view tutorials with Jupyter Notebook, install `jupyter` and `ipyparallel` in the same enviroment where Bodo is installed.

```shell
conda create -n Bodo python=3.9 -c conda-forge
conda activate Bodo
conda install bodo ipyparallel=8.1 jupyterlab=3 -c bodo.ai -c conda-forge
```

Clone or fork this repository. Go to the folder you want to clone, then clone it. 
```shell
cd <path-to-your-sandbox> # e.g., cd ~/mysandbox or cd username\sandbox\
git clone git@github.com:Bodo-inc/Bodo-tutorial.git
```

Now go to `Bodo-tutorial` folder and start the Jupyter Notebook:

```shell
    cd <path-to-Bodo-tutorial-folder> # e.g., cd ~/mysandbox/Bodo-tutorial
    jupyter notebook
```
    

Start with [`bodo_getting_started.ipynb`](bodo_getting_started.ipynb) 
and then move on to [`bodo_tutorial.ipynb`](bodo_tutorial.ipynb).

_________________________
More documentation can be found at http://docs.bodo.ai.

More Jupyter Notebook setup instructions for Bodo can be found [here](https://docs.bodo.ai/installation_and_setup/ipyparallel/#ipyparallelsetup).
