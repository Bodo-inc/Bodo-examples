"""
TPCH Query 1
    
    Usage: 
    mpiexec -n [cores] python q01.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem
import time
import argparse
import bodo
import pandas as pd


@bodo.jit
def q(data_folder):
    date = "1998-09-02"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    print("Reading time (s): ", time.time() - t1)
    bodo.barrier()
    t1 = time.time()
    sel = lineitem.L_SHIPDATE <= date
    flineitem = lineitem[sel].copy()  # copy is needed for the next two statements
    flineitem["AVG_QTY"] = flineitem.L_QUANTITY
    flineitem["AVG_PRICE"] = flineitem.L_EXTENDEDPRICE
    flineitem["DISC_PRICE"] = flineitem.L_EXTENDEDPRICE * (1 - flineitem.L_DISCOUNT)
    flineitem["CHARGE"] = (
        flineitem.L_EXTENDEDPRICE * (1 - flineitem.L_DISCOUNT) * (1 + flineitem.L_TAX)
    )
    gb = flineitem.groupby(["L_RETURNFLAG", "L_LINESTATUS"], as_index=False)[
        "L_QUANTITY",
        "L_EXTENDEDPRICE",
        "DISC_PRICE",
        "CHARGE",
        "AVG_QTY",
        "AVG_PRICE",
        "L_DISCOUNT",
        "L_ORDERKEY",
    ]
    total = gb.agg(
        {
            "L_QUANTITY": "sum",
            "L_EXTENDEDPRICE": "sum",
            "DISC_PRICE": "sum",
            "CHARGE": "sum",
            "AVG_QTY": "mean",
            "AVG_PRICE": "mean",
            "L_DISCOUNT": "mean",
            "L_ORDERKEY": "count",
        }
    )
    total = total.sort_values(["L_RETURNFLAG", "L_LINESTATUS"])
    print("Execution time (s): ", time.time() - t1)
    print(total)


def main():
    parser = argparse.ArgumentParser(description="tpch-q1")
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
