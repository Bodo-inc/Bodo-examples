"""
TPCH Query 12
    
    Usage: 
    mpiexec -n [cores] python q12.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_orders
import time
import argparse
import bodo
import pandas as pd


@bodo.jit(cache=True)
def q12(data_folder):
    date1 = "1994-01-01"
    date2 = "1995-01-01"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    sel = (
        (lineitem.L_RECEIPTDATE < date2)
        & (lineitem.L_COMMITDATE < date2)
        & (lineitem.L_SHIPDATE < date2)
        & (lineitem.L_SHIPDATE < lineitem.L_COMMITDATE)
        & (lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE)
        & (lineitem.L_RECEIPTDATE >= date1)
        & ((lineitem.L_SHIPMODE == "MAIL") | (lineitem.L_SHIPMODE == "SHIP"))
    )
    flineitem = lineitem[sel]
    jn = flineitem.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

    def g1(x):
        return ((x == "1-URGENT") | (x == "2-HIGH")).sum()

    def g2(x):
        return ((x != "1-URGENT") & (x != "2-HIGH")).sum()

    total = jn.groupby("L_SHIPMODE", as_index=False)["O_ORDERPRIORITY"].agg((g1, g2))
    total = total.sort_values("L_SHIPMODE")
    print("Execution time (s): ", time.time() - t1)
    print(total)


def main():
    parser = argparse.ArgumentParser(description="tpch-q12")
    parser.add_argument(
        "--folder",
        type=str,
        default="data/tpch-datagen/data",
        help="The folder containing TPCH data",
    )
    args = parser.parse_args()
    folder = args.folder
    q12(folder)


if __name__ == "__main__":
    main()
