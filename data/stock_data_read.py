"""
Generate data for Intraday Mean example

    Usage: python stock_data_read.py

Writes to stock_data_all_yahoo.hdf5
"""
import pandas as pd
import numpy as np
from pandas_datareader import data
import h5py
import os
import argparse


def main(symbols_file, output_file, num_symbols):
    stocks = pd.read_csv(symbols_file)
    f = h5py.File(output_file, "w")

    num_valid_symbols = 0
    for symbol in stocks.Symbol:
        try:
            df = data.DataReader(symbol, "yahoo", start="1/1/1950")
        except:
            # Some symbols may not be found. Skip those symbols and
            # exclude them from the symbols count
            continue
        num_valid_symbols += 1
        N = len(df)
        grp = f.create_group(symbol)
        grp.create_dataset("Open", (N,), dtype="f8")[:] = df["Open"]
        grp.create_dataset("High", (N,), dtype="f8")[:] = df["High"]
        grp.create_dataset("Low", (N,), dtype="f8")[:] = df["Low"]
        grp.create_dataset("Close", (N,), dtype="f8")[:] = df["Close"]
        grp.create_dataset("Volume", (N,), dtype="f8")[:] = df["Volume"]
        grp.create_dataset("Date", (N,), dtype="i8")[:] = df.index.values.astype(
            np.int64
        )
        if num_valid_symbols >= num_symbols:
            break

    f.close()


if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parser = argparse.ArgumentParser(
        description="Generate data for intraday_mean stock market example"
    )
    parser.add_argument("--symbols_file", dest="symbols_file", type=str, default=f"{dir_path}/all_syms.csv")
    parser.add_argument("--output_file", dest="output_file", type=str, default=f"{dir_path}/stock_data_all_yahoo.hdf5")
    parser.add_argument("--num_symbols", dest="num_symbols", type=int, default=100)
    args = parser.parse_args()
    main(args.symbols_file, args.output_file, args.num_symbols)
