"""
TPCH Query 6

    Usage:
    mpiexec -n [cores] python q06_bodosql.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""

import argparse
import time

import bodo
import bodosql

from loader import load_lineitem


@bodo.jit
def q(data_folder):
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    bc = bodosql.BodoSQLContext({"lineitem": lineitem, })

    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    tpch_query = """select
                      sum(l_extendedprice * l_discount) as revenue
                    from
                      lineitem
                    where
                      l_shipdate >= '1994-01-01'
                      and l_shipdate < '1995-01-01'
                      and l_discount between 0.05 and 0.07
                      and l_quantity < 24
    """
    res = bc.sql(tpch_query)
    print("Execution time (s): ", time.time() - t1)
    print(res)


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
