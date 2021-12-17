import inspect
import os
import time

import bodo
import config as config
import ipyparallel as ipp
import numpy as np
import pandas as pd
import psutil
import streamlit as st

pq_file_path = config.LOCAL_FILE_PATH

st.title('Scale up your datasets and make Pandas fly with Bodo!')
st.subheader('Based on Streamlit example for Uber pickups in NYC')
st.subheader(' - > Basic Info')
st.subheader('Number of physical cores/ranks available on system: %s' % psutil.cpu_count(logical=False))


def load_data_pandas(pq_file_path, date_col='date/time'):
    data = pd.read_parquet(pq_file_path)
    data[date_col] = pd.to_datetime(data[date_col])
    return data


@bodo.jit(returns_maybe_distributed=False, cache=True)
def load_data_bodo(pq_file_path, date_col='date/time'):
    data = pd.read_parquet(pq_file_path)
    data[date_col] = pd.to_datetime(data[date_col])
    return bodo.gatherv(data)


def build_main(pq_file_path, date_col='date/time'):
    op_df = load_data_bodo(pq_file_path, date_col='Date/Time')
    return op_df


def initialize_bodo(pq_file_path, date_col='date/time'):
    t0 = time.time()

    client = ipp.Client(profile='mpi')
    dview = client[:]
    # import libraries
    dview.execute("import numpy as np")
    dview.execute("import pandas as pd")
    dview.execute("import bodo")
    dview.execute("import time")
    dview.execute("import os")
    dview.execute("import datetime as dt")
    dview.execute("import sys")

    if dview.apply_sync(os.getcwd)[0] != config.home_dir:
        print('CWD is: ', dview.apply_sync(os.getcwd)[0])
        dview.map(os.chdir, [config.home_dir] * 30)
        print('CHANGED CWD to: ', dview.apply_sync(os.getcwd)[0])

    bodo_funcs = [load_data_bodo]

    for f in bodo_funcs:
        # get source code of Bodo function
        f_src = inspect.getsource(f)
        # execute the source code thereby defining the function on engines
        dview.execute(f_src).get()

    op_df = dview.apply(build_main, pq_file_path, 'Date/Time').get()

    t1 = time.time()
    print("Total Exec + Compilation time:", t1 - t0)
    client.close()

    return op_df[0]


t0 = time.time()
pdf = load_data_pandas(pq_file_path, date_col='Date/Time')
t1 = time.time()
st.subheader('Pandas df')
st.subheader('Time taken for one op with Pandas:')
st.subheader(t1 - t0)

st.write(pdf.head(2))

t2 = time.time()
bdf = initialize_bodo(pq_file_path, date_col='Date/Time')
t3 = time.time()
st.subheader('Bodo df')
st.subheader('Total Compilation and Execution time taken for one op with Bodo:')
st.subheader(t3 - t2)
st.write(bdf.head(2))

DATE_COLUMN = 'date/time'
lowercase = lambda x: str(x).lower()
bdf.rename(lowercase, axis='columns', inplace=True)

st.subheader('Number of pickups by hour')
hist_values = np.histogram(bdf[DATE_COLUMN].dt.hour, bins=24, range=(0, 24))[0]
st.bar_chart(hist_values)
