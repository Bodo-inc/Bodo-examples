"""
Logistic Regression example to demonstrate Numpy functionality.

    Usage: mpiexec -n [cores] python logistic_regression.py --file [filename] --iterations [iterations]

See data generation script in data/logistic_regression_datagen.py
Setting "export OMP_NUM_THREADS=1" is recommended to avoid interference from threads
in Numpy's math library (e.g. MKL).
"""
import bodo
import numpy as np
import h5py
import argparse
import time


#@bodo.jit
def logistic_regression(iterations, fname):
    f = h5py.File(fname, "r")
    X = f["points"][:]
    Y = f["responses"][:]
    f.close()
    D = X.shape[1]
    w = np.ones(D) - 0.5
    t1 = time.time()
    for i in range(iterations):
        w -= np.dot(((1.0 / (1.0 + np.exp(-Y * np.dot(X, w))) - 1.0) * Y), X)
    t2 = time.time()
    print("Execution time:", t2 - t1, "\nresult:", w)
    return w


def main():
    parser = argparse.ArgumentParser(description="Logistic Regression example")
    parser.add_argument("--file", dest="file", type=str, default="data/lr.hdf5")
    parser.add_argument("--iterations", dest="iterations", type=int, default=20)
    args = parser.parse_args()
    file_name = args.file
    iterations = args.iterations
    _w = logistic_regression(iterations, file_name)


if __name__ == "__main__":
    main()
