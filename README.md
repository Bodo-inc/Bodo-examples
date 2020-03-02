# Bodo-examples

Welcome to Bodo examples!

First make sure you have Bodo [installed](http://docs.bodo.ai/latest/source/install.html).

Other packages that are required for certain data generations: `pandas_datareader` and `scikit-learn`:
	
	conda install -c conda-forge pandas-datareader
	conda install -c conda-forge scikit-learn

To try the examples, lets use `pi.py`:

    # run the example on a single core
    python examples/pi.py
    # run the example on 4 cores
    mpiexec -n python examples/pi.py
 
Another example with the linear regression example:

	# generate data
	data/linear_regression_datagen.py
	# run example on 4 cores
	mpiexec -n python examples/linear_regression.py

For more information on each data generation and example, look at the docstring at the top of each python script.

_________________________
More documentation can be found at http://docs.bodo.ai.

Bodo tutorial can be found [here](https://github.com/Bodo-inc/Bodo-tutorial)
