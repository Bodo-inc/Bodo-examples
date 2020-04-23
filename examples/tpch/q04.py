"""
TPCH Query 4
    
    Usage: 
    mpiexec -n [cores] python q04.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_orders
import time
import argparse
import bodo
import pandas as pd


@bodo.jit
def q(data_folder):
    date1 = "1993-11-01"
    date2 = "1993-08-01"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    print("Reading time: ", ((time.time() - t1) * 1000), " (ms)")
    bodo.barrier()
    t1 = time.time()
    lsel = lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
    osel = (orders.O_ORDERDATE < date1) & (orders.O_ORDERDATE >= date2)
    flineitem = lineitem[lsel]
    forders = orders[osel]
    jn = forders[forders["O_ORDERKEY"].isin(flineitem["L_ORDERKEY"])]
    total = (
        jn.groupby("O_ORDERPRIORITY", as_index=False)["O_ORDERKEY"]
        .count()
        .sort_values(["O_ORDERPRIORITY"])
    )
    print("Execution time: ", ((time.time() - t1) * 1000), " (ms)")
    print(total)


def main():
    parser = argparse.ArgumentParser(description="tpch-q4")
    parser.add_argument(
        "--folder",
        type=str,
        default="data/tpch-datagen/data",
        help="The folder containing TPCH data",
    )
    args = parser.parse_args()
    folder = args.folder
    q(folder)


if __name__ == "__main__":
    main()
