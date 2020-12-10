"""
TPCH Query 6
    
    Usage: 
    mpiexec -n [cores] python q06.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem
import time
import argparse
import bodo
import pandas as pd


@bodo.jit(cache=True)
def q06(data_folder):
    date1 = "1996-01-01"
    date2 = "1997-01-01"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    sel = (
        (lineitem.L_SHIPDATE >= date1)
        & (lineitem.L_SHIPDATE < date2)
        & (lineitem.L_DISCOUNT >= 0.08)
        & (lineitem.L_DISCOUNT <= 0.1)
        & (lineitem.L_QUANTITY < 24)
    )
    flineitem = lineitem[sel]
    total = (flineitem.L_EXTENDEDPRICE * flineitem.L_DISCOUNT).sum()
    print("Execution time (s): ", time.time() - t1)
    print(total)


def main():
    parser = argparse.ArgumentParser(description="tpch-q6")
    parser.add_argument(
        "--folder",
        type=str,
        default="data/tpch-datagen/data",
        help="The folder containing TPCH data",
    )
    args = parser.parse_args()
    folder = args.folder
    q06(folder)


if __name__ == "__main__":
    main()
