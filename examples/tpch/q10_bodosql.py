"""
TPCH Query 10

    Usage:
    mpiexec -n [cores] python q10_bodosql.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_orders, load_customer, load_nation
import time
import argparse
import bodo
import bodosql


def q(data_folder):
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    nation = load_nation(data_folder)
    bc = bodosql.BodoSQLContext({"customer": customer,
                                 "orders": orders,
                                 "lineitem": lineitem,
                                 "nation": nation,
                                 })

    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    tpch_query = """select
                      c_custkey,
                      c_name,
                      sum(l_extendedprice * (1 - l_discount)) as revenue,
                      c_acctbal,
                      n_name,
                      c_address,
                      c_phone,
                      c_comment
                    from
                      customer,
                      orders,
                      lineitem,
                      nation
                    where
                      c_custkey = o_custkey
                      and l_orderkey = o_orderkey
                      and o_orderdate >= '1993-10-01'
                      and o_orderdate < '1994-01-01'
                      and l_returnflag = 'R'
                      and c_nationkey = n_nationkey
                    group by
                      c_custkey,
                      c_name,
                      c_acctbal,
                      c_phone,
                      n_name,
                      c_address,
                      c_comment
                    order by
                      revenue desc,
                      c_custkey
                    limit 20
    """
    res = bc.sql(tpch_query)
    print("Execution time (s): ", time.time() - t1)
    print(res)


def main():
    parser = argparse.ArgumentParser(description="tpch-q10")
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
