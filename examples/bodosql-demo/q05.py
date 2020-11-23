from query_setup import run_tpch_query
import argparse


def q5():
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
    run_tpch_query(tpch_query, False)


def main():
    parser = argparse.ArgumentParser(description="tpch-q3")
    parser.add_argument(
        "--folder",
        type=str,
        default="data/tpch-parquet/",
        help="The folder containing TPCH data in parquetized format",
    )
    args = parser.parse_args()
    folder = args.folder
    q5(folder)


if __name__ == "__main__":
    main()
