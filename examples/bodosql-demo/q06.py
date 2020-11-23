from query_setup import run_tpch_query
import argparse


def q6():
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
    run_tpch_query(tpch_query)


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
    q6(folder)


if __name__ == "__main__":
    main()
