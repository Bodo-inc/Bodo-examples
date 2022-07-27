# Bodo Examples

This repository contains code examples demonstrating how Bodo accelerates and scales
Pandas and Scikit-learn workloads automatically. All examples can be run on a local laptop
using Bodo Community Edition: `pip install bodo`.

In addition, there are examples for using Bodo with data infrastructure tools such as Docker, Terraform, Kubernetes and Streamlit.
Feedback is appreciated.



## Running Example Scripts

First make sure you have Bodo [installed](https://docs.bodo.ai/installation_and_setup/install).
`scikit-learn` is also required for the ML examples.

Many of the example scripts can take in optional arguments.
`python <path>/<script.py> --help` shows the usage.

By default all examples scripts can be run from the
top directory (Bodo-examples) without any changes.
Otherwise, make sure to change path of data files.
For more information on the examples,
please see the docstring at the top of each python script.

- [TPCH Queries](https://github.com/Bodo-inc/Bodo-examples/tree/master/examples/tpch), more information on TPC-H can be found [here](http://www.tpc.org/tpch/)

- [Beer Reviews](examples/beer-reviews/beer-reviews.py)

- [NYC Parking Tickets](examples/nyc-parking/nyc-parking.py)

- [NYC Taxi](examples/nyc-taxi):
    - [Daily Pickups](examples/nyc-taxi/get_daily_pickups.py)
    - [JFK Hourly Pickups](examples/nyc-taxi/jfk_hourly_pickups.py)
    - [Monthly Travel Times](examples/nyc-taxi/monthly_taxi_travel_times.py)
    - [Weekday Pickup and Dropoff](examples/nyc-taxi/weekday_taxi_trips_by_pickup_and_dropoff.py)

## Try the Examples

An example performing beer reviews example:

    # run example on 8 cores
    mpiexec -n 8 python examples/beer-reviews/beer-reviews.py


---------------------------
More documentation can be found at http://docs.bodo.ai.

Bodo tutorial can be found [here](https://github.com/Bodo-inc/Bodo-tutorial).


## Launch Using Binder

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Bodo-inc/Bodo-examples/HEAD)
