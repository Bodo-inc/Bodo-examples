# Bodo Examples

Welcome to Bodo examples!

## Install required packages

First make sure you have Bodo [installed](http://docs.bodo.ai/latest/source/install.html).

Other packages that are required to run the data generation scripts, `pandas_datareader` and `scikit-learn`:
	
	conda install -c conda-forge pandas-datareader
	conda install -c conda-forge scikit-learn

## Examples and corresponding data generation

Many of the data generation scripts and example scripts can take in optional arguments. 
`python path/script.py --help` shows the usage.

By default all examples and data generation scripts can be run from home directory (Bodo-examples) without any changes. Otherwise, make sure to change path of data files.

For more information on data generation and examples, please see the docstring at the top of each python script.

- [Kernel Density Estimation](https://github.com/Bodo-inc/Bodo-examples/blob/master/examples/kernel_density_estimation.py)
  - [data generation](https://github.com/Bodo-inc/Bodo-examples/blob/master/data/kde_datagen.py)
- [Intraday Mean](https://github.com/Bodo-inc/Bodo-examples/blob/master/examples/intraday_mean.py)
  - [data generation](https://github.com/Bodo-inc/Bodo-examples/blob/master/data/stock_data_read.py)
- [Some TPCH Queries](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch), more information on TPC-H can be found [here](http://www.tpc.org/tpch/): 
Query #[1](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q01.py),
[3](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q03.py),
[4](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q04.py),
[5](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q05.py),
[6](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q06.py),
[9](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q09.py),
[10](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q10.py),
[12](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q12.py), 
[14](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q14.py), 
[18](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q18.py), 
[19](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q19.py), 
[20](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q20.py).
  - data generation: generated data will be available at `data/tpch-datagen/data`:

		# To generate data with a scale of 1, equivalent to 1GB of data
		data/tpch-datagen/generateData.sh 1
		# To generate data with a scale of 2, equivalent to 2GB of data
		data/tpch-datagen/generateData.sh 2

- [Beer Reviews](examples/beer-reviews/beer-reviews.py)

- [NYC Parking Tickets](examples/nyc-parking/nyc-parking.py)

- [NYC Taxi](examples/nyc-taxi):
    - [Daily Pickups](examples/nyc-taxi/get_daily_pickups.py)
    - [JFK Hourly Pickups](examples/nyc-taxi/jfk_hourly_pickups.py)
    - [Monthly Travel Times](examples/nyc-taxi/monthly_taxi_travel_times.py)
    - [Weekday Pickup and Dropoff](examples/nyc-taxi/weekday_taxi_trips_by_pickup_and_dropoff.py)

- [Monte Carlo Pi Calculation](examples/miscellaneous/pi.py)
- [k-means](examples/miscellaneous/k-means.py)
  - [data generation](https://github.com/Bodo-inc/Bodo-examples/blob/master/data/logistic_regression_datagen.py)
- [Linear Regression](examples/miscellaneous/linear_regression.py)
  - [data generation](https://github.com/Bodo-inc/Bodo-examples/blob/master/data/linear_regression_datagen.py)
- [Logistic Regression](examples/miscellaneous/logistic_regression.py)
  - [data generation](https://github.com/Bodo-inc/Bodo-examples/blob/master/data/logistic_regression_datagen.py)

## Try the examples


An example performing TPCH query #1:

	# generate data
	data/tpch-datagen/generateData.sh 1
	# run example on 4 cores
	mpiexec -n 4 python examples/tpch/q01.py

An example performing beer reviews example:

    # run example on 4 cores
    mpiexec -n 4 python examples/beer-reviews/beer-reviews.py

An example performing Monte Carlo Pi Calculation:

    # run the example on a single core
    python examples/pi.py
    # run the example on 4 cores
    mpiexec -n 4 python examples/pi.py
 
An example performing linear regression:

	# generate data
	python data/linear_regression_datagen.py
	# run example on 4 cores
	mpiexec -n 4 python examples/linear_regression.py

_________________________

## BodoSQL examples

BodoSQL is in beta stage and not available to the public yet. [Contact us](https://bodo.ai/contact/) for information about trial access.


BodoSQL examples include TPCH queries Q3, Q5, Q6 and Q10.


 [BodoSQL example TPCH Queries](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch) : 
Query #[3](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q03_bodosql.py),
[5](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q05.py),
[6](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q06_bodosql.py),
[10](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch/q10.py)
---------------------------
More documentation can be found at http://docs.bodo.ai.

Bodo tutorial can be found [here](https://github.com/Bodo-inc/Bodo-tutorial).
