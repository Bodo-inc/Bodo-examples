"""
Kernel Density Estimation example to demonstrate Numpy functionality and explicit
parallel loops with reduction.

    Usage: mpiexec -n [cores] python kernel_density_estimation.py --file [filename]
    See data generation script in data/kde_datagen.py
"""
import pandas as pd
import numpy as np
import bodo
from bodo import prange
import argparse
import time


@bodo.jit
def kde(fname):
    t1 = time.time()
    df = pd.read_parquet(fname)
    X = df["points"].values
    b = 0.5
    points = np.array([-1.0, 2.0, 5.0])
    N = len(points)
    n = len(X)
    exps = 0
    for i in prange(n):
        p = X[i]
        d = (-((p - points) ** 2)) / (2 * b ** 2)
        m = np.min(d)
        exps += m - np.log(b * N) + np.log(np.sum(np.exp(d - m)))
    print("Execution time:", time.time() - t1, "\nresult:", exps)
    return exps


def main():
    parser = argparse.ArgumentParser(description="Kernel Density Estimation")
    parser.add_argument("--file", dest="file", type=str, default="data/kde.pq")
    args = parser.parse_args()
    filename = args.file
    _res = kde(filename)


if __name__ == "__main__":
    main()
