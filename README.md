# Bodo-examples

Welcome to Bodo examples!

First make sure you have Bodo [installed](http://docs.bodo.ai/latest/source/install.html).

Other packages that are required to run the data generation scripts, `pandas_datareader` and `scikit-learn`:
	
	conda install -c conda-forge pandas-datareader
	conda install -c conda-forge scikit-learn

To try the examples, let's use `pi.py`:

    # run the example on a single core
    python examples/pi.py
    # run the example on 4 cores
    mpiexec -n 4 python examples/pi.py
 
An example performing linear regression:

	# generate data
	data/linear_regression_datagen.py
	# run example on 4 cores
	mpiexec -n 4 python examples/linear_regression.py

For more information on data generation and examples, please see the docstring at the top of each python script.

_________________________
More documentation can be found at http://docs.bodo.ai.

Bodo tutorial can be found [here](https://github.com/Bodo-inc/Bodo-tutorial)
