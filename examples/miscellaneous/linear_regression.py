"""
Linear Regression example to demonstrate Numpy functionality.

    Usage: mpiexec -n [cores] python linear_regression.py --file [filename] --iterations [iterations]

See data generation script in data/linear_regression_datagen.py
Setting "export OMP_NUM_THREADS=1" is recommended to avoid interference from threads
in Numpy's math library (e.g. MKL).
"""
import bodo
import numpy as np
import h5py
import argparse
import time


@bodo.jit
def linear_regression(iterations, fname):
    f = h5py.File(fname, "r")
    X = f["points"][:]
    Y = f["responses"][:]
    f.close()
    N, D = X.shape
    p = Y.shape[1]
    alphaN = 0.01 / N
    w = np.zeros((D, p))
    t1 = time.time()
    for i in range(iterations):
        w -= alphaN * np.dot(X.T, np.dot(X, w) - Y)
    t2 = time.time()
    print("Execution time:", t2 - t1, "\nresult:", w)
    return w


def main():
    parser = argparse.ArgumentParser(description="Linear Regression example")
    parser.add_argument("--file", dest="file", type=str, default="data/slir.hdf5")
    parser.add_argument("--iterations", dest="iterations", type=int, default=30)
    args = parser.parse_args()
    file_name = args.file
    iterations = args.iterations

    _w = linear_regression(iterations, file_name)


if __name__ == "__main__":
    main()
