"""
TPCH Query 5

    Usage:
    mpiexec -n [cores] python q05_bodosql.py --folder [folder]

Run data/tpch-datagen/generateData.sh to generate TPCH database.
"""
from loader import load_lineitem, load_orders, load_customer, load_nation, load_region, load_supplier
import time
import argparse
import bodo
import bodosql


@bodo.jit
def q(data_folder):
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    nation = load_nation(data_folder)
    region = load_region(data_folder)
    supplier = load_supplier(data_folder)
    bc = bodosql.BodoSQLContext({"customer": customer,
                                 "orders": orders,
                                 "lineitem": lineitem,
                                 "nation": nation,
                                 "region": region,
                                 "supplier": supplier,
                                 })

    print("Reading time (s): ", time.time() - t1)
    t1 = time.time()
    tpch_query = """select
                      n_name,
                      sum(l_extendedprice * (1 - l_discount)) as revenue
                    from
                      customer,
                      orders,
                      lineitem,
                      supplier,
                      nation,
                      region
                    where
                      c_custkey = o_custkey
                      and l_orderkey = o_orderkey
                      and l_suppkey = s_suppkey
                      and c_nationkey = s_nationkey
                      and s_nationkey = n_nationkey
                      and n_regionkey = r_regionkey
                      and r_name = 'ASIA'
                      and o_orderdate >= '1994-01-01'
                      and o_orderdate < '1995-01-01'
                    group by
                      n_name
                    order by
                      revenue desc
    """
    res = bc.sql(tpch_query)
    print("Execution time (s): ", time.time() - t1)
    print(res)


def main():
    parser = argparse.ArgumentParser(description="tpch-q5")
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
