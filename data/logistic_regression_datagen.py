"""
Generate data for Logistic Regression example

    Usage: python logistic_regression_datagen.py --file [filename] --samples [samples] --feature [features]
"""
import h5py
import numpy as np
import argparse
import sklearn.datasets


def gen_lr(N, D, file_name):
    points, responses = sklearn.datasets.make_classification(N, D)
    f = h5py.File(file_name, "w")
    dset1 = f.create_dataset("points", (N, D), dtype="f8")
    dset1[:] = points
    dset2 = f.create_dataset("responses", (N,), dtype="f8")
    dset2[:] = responses.astype(np.float64)
    f.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate data for Logistic Regression example"
    )
    parser.add_argument("--samples", dest="samples", type=int, default=20000000)
    parser.add_argument("--features", dest="features", type=int, default=10)
    parser.add_argument("--file", dest="file", type=str, default="./lr.hdf5")
    args = parser.parse_args()
    N = args.samples
    D = args.features
    file_name = args.file

    gen_lr(N, D, file_name)


if __name__ == "__main__":
    main()
