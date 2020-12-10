"""
TPCH Query 14
    
    Usage: 
    mpiexec -n [cores] python q14.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_part
import time
import argparse
import bodo
import pandas as pd


@bodo.jit(cache=True)
def q14(data_folder):
    startDate = "1994-03-01"
    endDate = "1994-04-01"
    p_type_like = "PROMO"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    part = load_part(data_folder)
    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    sel = (lineitem.L_SHIPDATE >= startDate) & (lineitem.L_SHIPDATE < endDate)
    flineitem = lineitem[sel]
    jn = flineitem.merge(part, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jn["TMP"] = jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)
    total = jn[jn.P_TYPE.str.startswith(p_type_like)].TMP.sum() * 100 / jn.TMP.sum()
    print("Execution time (s): ", time.time() - t1)
    print(total)


def main():
    parser = argparse.ArgumentParser(description="tpch-q14")
    parser.add_argument(
        "--folder",
        type=str,
        default="data/tpch-datagen/data",
        help="The folder containing TPCH data",
    )
    args = parser.parse_args()
    folder = args.folder
    q14(folder)


if __name__ == "__main__":
    main()
