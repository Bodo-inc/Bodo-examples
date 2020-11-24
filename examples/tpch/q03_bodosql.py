"""
TPCH Query 3

    Usage:
    mpiexec -n [cores] python q03_bodosql.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_orders, load_customer
import time
import argparse
import bodo
import bodosql


def q(data_folder):
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    bc = bodosql.BodoSQLContext({"customer": customer,
                                 "orders": orders,
                                 "lineitem": lineitem,
                                 })
    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    tpch_query = """select
                      l_orderkey,
                      sum(l_extendedprice * (1 - l_discount)) as revenue,
                      o_orderdate,
                      o_shippriority
                    from
                      customer,
                      orders,
                      lineitem
                    where
                      c_mktsegment = 'BUILDING'
                      and c_custkey = o_custkey
                      and l_orderkey = o_orderkey
                      and o_orderdate < '1995-03-15'
                      and l_shipdate > '1995-03-15'
                    group by
                      l_orderkey,
                      o_orderdate,
                      o_shippriority
                    order by
                      revenue desc,
                      o_orderdate,
                      l_orderkey
                    limit 10
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
