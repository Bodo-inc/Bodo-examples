"""
TPCH Query 18
    
    Usage: 
    mpiexec -n [cores] python q18.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import *
import time
import argparse
import bodo
import pandas as pd


@bodo.jit
def q(data_folder):
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    print("Reading time: ", ((time.time() - t1) * 1000), " (ms)")
    bodo.barrier()
    t1 = time.time()
    gb1 = lineitem.groupby("L_ORDERKEY", as_index = False)["L_QUANTITY"].sum()
    fgb1 = gb1[gb1.L_QUANTITY > 300]
    jn1 = fgb1.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
    gb2 = jn2.groupby(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", 
                        "O_ORDERDATE", "O_TOTALPRICE"], as_index = False)["L_QUANTITY"].sum()
    total = gb2.sort_values(["O_TOTALPRICE", "O_ORDERDATE"], ascending = [False, True])
    print("Execution time: ", ((time.time() - t1) * 1000), " (ms)")
    print(total.head(100))

def main():
    parser = argparse.ArgumentParser(description="tpch-q18")
    parser.add_argument("--folder", type=str, default='data/tpch-datagen/data', help="The folder containing TPCH data")
    args = parser.parse_args()
    folder = args.folder
    q(folder)


if __name__ == "__main__":
    main()
