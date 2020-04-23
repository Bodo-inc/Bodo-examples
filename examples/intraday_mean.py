"""
Intraday example to demonstrate Pandas functionality.
    
    Usage: 
    mpiexec -n [cores] python intraday_mean.py --file [file] --maxDays [max_num_days]

# adopted from:
# http://www.pythonforfinance.net/2017/02/20/intraday-stock-mean-reversion-trading-backtest-in-python/
See data generation script in data/stock_data_read.py
"""
import pandas as pd
import numpy as np
import h5py
import argparse
import time
import bodo
from bodo import prange


@bodo.jit(
    # More information on 'locals' in the bodo decorator
    # http://docs.bodo.ai/latest/source/user_guide.html#input-array-types
    locals={
        "s_open": bodo.float64[:],
        "s_high": bodo.float64[:],
        "s_low": bodo.float64[:],
        "s_close": bodo.float64[:],
        "s_vol": bodo.float64[:],
    }
)
def intraday_mean_revert(file_name, max_num_days):
    f = h5py.File(file_name, "r")
    sym_list = list(f.keys())
    nsyms = len(sym_list)
    all_res = np.zeros(max_num_days)

    t1 = time.time()

    # More information on bodo's explicit parallel loop: prange
    # http://docs.bodo.ai/latest/source/user_guide.html#explicit-parallel-loops
    for i in prange(nsyms):
        symbol = sym_list[i]

        s_open = f[symbol + "/Open"][:]
        s_high = f[symbol + "/High"][:]
        s_low = f[symbol + "/Low"][:]
        s_close = f[symbol + "/Close"][:]
        s_vol = f[symbol + "/Volume"][:]
        df = pd.DataFrame(
            {
                "Open": s_open,
                "High": s_high,
                "Low": s_low,
                "Close": s_close,
                "Volume": s_vol,
            }
        )

        # create column to hold our 90 day rolling standard deviation
        df["Stdev"] = df["Close"].rolling(window=90).std()

        # create a column to hold our 20 day moving average
        df["Moving Average"] = df["Close"].rolling(window=20).mean()

        # create a column which holds a TRUE value if the gap down from previous day's low to next
        # day's open is larger than the 90 day rolling standard deviation
        df["Criteria1"] = (df["Open"] - df["Low"].shift(1)) < -df["Stdev"]

        # create a column which holds a TRUE value if the opening price of the stock is above the 20 day moving average
        df["Criteria2"] = df["Open"] > df["Moving Average"]

        # create a column that holds a TRUE value if both above criteria are also TRUE
        df["BUY"] = df["Criteria1"] & df["Criteria2"]

        # calculate daily % return series for stock
        df["Pct Change"] = (df["Close"] - df["Open"]) / df["Open"]

        # create a strategy return series by using the daily stock returns where the trade criteria above are met
        df["Rets"] = df["Pct Change"][df["BUY"] == True]

        n_days = len(df["Rets"])
        res = np.zeros(max_num_days)
        if n_days:
            res[-n_days:] = df["Rets"].fillna(0).values
        all_res += res

    f.close()
    print(all_res.mean())
    print("execution time:", time.time() - t1)


def main():
    parser = argparse.ArgumentParser(description="Intraday Mean example")
    parser.add_argument(
        "--file", dest="file", type=str, default="data/stock_data_all_yahoo.hdf5"
    )
    parser.add_argument("--maxDays", dest="max_num_days", type=int, default=14513)
    args = parser.parse_args()
    file_name = args.file
    max_num_days = args.max_num_days
    intraday_mean_revert(file_name, max_num_days)


if __name__ == "__main__":
    main()
