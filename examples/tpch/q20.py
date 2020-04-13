"""
TPCH Query 20
    
    Usage: 
    mpiexec -n [cores] python q20.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import *
import time
import argparse
import bodo
import pandas as pd


@bodo.jit
def q(data_folder):
    date1 = "1996-01-01"
    date2 = "1997-01-01"
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    part = load_part(data_folder)
    nation = load_nation(data_folder)
    partsupp = load_partsupp(data_folder)
    supplier = load_supplier(data_folder)
    print("Reading time: ", ((time.time() - t1) * 1000), " (ms)")
    bodo.barrier()
    t1 = time.time()
    psel = part.P_NAME.str.startswith("azure")
    nsel = nation.N_NAME == "JORDAN"
    lsel = (lineitem.L_SHIPDATE >= date1) & (lineitem.L_SHIPDATE < date2)
    fpart = part[psel]
    fnation = nation[nsel]
    flineitem = lineitem[lsel]
    jn1 = fpart.merge(partsupp, left_on="P_PARTKEY", right_on="PS_PARTKEY")
    jn2 = jn1.merge(flineitem, left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"])
    gb = jn2.groupby(['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY'], as_index = False)["L_QUANTITY"].sum()
    gbsel = gb.PS_AVAILQTY > (0.5 * gb.L_QUANTITY)
    fgb = gb[gbsel]
    jn3 = fgb.merge(supplier, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
    jn4 = fnation.merge(jn3, left_on="N_NATIONKEY", right_on="S_NATIONKEY")
    jn4 = jn4[['S_NAME', 'S_ADDRESS']]
    total = jn4.sort_values('S_NAME').drop_duplicates()
    print("Execution time: ", ((time.time() - t1) * 1000), " (ms)")
    print(total)

def main():
    parser = argparse.ArgumentParser(description="tpch-q20")
    parser.add_argument("--folder", type=str, default='data/tpch-datagen/data', help="The folder containing TPCH data")
    args = parser.parse_args()
    folder = args.folder
    q(folder)


if __name__ == "__main__":
    main()
