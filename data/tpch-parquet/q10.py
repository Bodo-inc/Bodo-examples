from query_setup import run_tpch_query
import argparse


def q10():
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
    q10(folder)


if __name__ == "__main__":
    main()
