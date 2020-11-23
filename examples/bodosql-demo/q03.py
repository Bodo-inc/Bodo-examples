from query_setup import run_tpch_query
import argparse


def q3(dir_name):
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
    run_tpch_query(dir_name, tpch_query)


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
    q3(folder)


if __name__ == "__main__":
    main()
