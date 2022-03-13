# Bodo Examples

Welcome to Bodo examples!

## Install required packages

First make sure you have Bodo [installed](https://docs.bodo.ai/latest/source/installation_and_setup/index.html).

Other packages that are required to run the data generation scripts, `pandas_datareader` and `scikit-learn`:
	
	conda install -c conda-forge pandas-datareader
	conda install -c conda-forge scikit-learn

## Examples and corresponding data generation

Many of the data generation scripts and example scripts can take in optional arguments. 
`python path/script.py --help` shows the usage.

By default all examples and data generation scripts can be run from home directory (Bodo-examples) without any changes. Otherwise, make sure to change path of data files.

For more information on data generation and examples, please see the docstring at the top of each python script.

- [TPCH Queries](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch), more information on TPC-H can be found [here](http://www.tpc.org/tpch/)
  - data generation: generated data will be available at `examples/tpch/pch-datagen`:
  
        # To generate data with a scale of 1, equivalent to 1GB of data
        tpch-datagen/generateData.sh 1
        # To generate data with a scale of 2, equivalent to 2GB of data
        tpch-datagen/generateData.sh 2

- [Beer Reviews](examples/beer-reviews/beer-reviews.py)

- [NYC Parking Tickets](examples/nyc-parking/nyc-parking.py)

- [NYC Taxi](examples/nyc-taxi):
    - [Daily Pickups](examples/nyc-taxi/get_daily_pickups.py)
    - [JFK Hourly Pickups](examples/nyc-taxi/jfk_hourly_pickups.py)
    - [Monthly Travel Times](examples/nyc-taxi/monthly_taxi_travel_times.py)
    - [Weekday Pickup and Dropoff](examples/nyc-taxi/weekday_taxi_trips_by_pickup_and_dropoff.py)

## Try the examples

An example performing beer reviews example:

    # run example on 4 cores
    mpiexec -n 4 python examples/beer-reviews/beer-reviews.py


---------------------------
More documentation can be found at http://docs.bodo.ai.

Bodo tutorial can be found [here](https://github.com/Bodo-inc/Bodo-tutorial).


## Launch using Binder

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Bodo-inc/Bodo-examples/HEAD)
