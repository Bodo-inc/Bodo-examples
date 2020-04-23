"""
TPCH Query 3
    
    Usage: 
    mpiexec -n [cores] python q03.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_orders, load_customer
import time
import argparse
import bodo
import pandas as pd


@bodo.jit
def q(data_folder):
    date = "1995-03-04"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    print("Reading time: ", ((time.time() - t1) * 1000), " (ms)")
    bodo.barrier()
    t1 = time.time()
    lsel = lineitem.L_SHIPDATE > date
    osel = orders.O_ORDERDATE < date
    csel = customer.C_MKTSEGMENT == "HOUSEHOLD"
    flineitem = lineitem[lsel]
    forders = orders[osel]
    fcustomer = customer[csel]
    jn1 = fcustomer.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn2 = jn1.merge(flineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    jn2["TMP"] = jn2.L_EXTENDEDPRICE * (1 - jn2.L_DISCOUNT)
    total = (
        jn2.groupby(["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"], as_index=False)[
            "TMP"
        ]
        .sum()
        .sort_values(["TMP"], ascending=False)
    )
    res = total[["L_ORDERKEY", "TMP", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    print("Execution time: ", ((time.time() - t1) * 1000), " (ms)")
    print(res.head(10))


def main():
    parser = argparse.ArgumentParser(description="tpch-q3")
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
