"""
Sets up dataframes after reading parquet files of data generated TPCH Benchmark on BodoSQL.
"""

import bodo
import bodosql
import pandas as pd


@bodo.jit()
def load_tpch_data(dir_name):
    """ Load the necessary TPCH dataframes given a root directory
    """
    customer_df = pd.read_parquet(dir_name + "/customer.parquet/")
    orders_df = pd.read_parquet(dir_name + "/orders.parquet/")
    lineitem_df = pd.read_parquet(dir_name + "/lineitem.parquet/")
    nation_df = pd.read_parquet(dir_name + "/nation.parquet/")
    region_df = pd.read_parquet(dir_name + "/region.parquet/")
    supplier_df = pd.read_parquet(dir_name + "/supplier.parquet/")
    return customer_df, orders_df, lineitem_df, nation_df, region_df, supplier_df


def run_tpch_query(dir_name, tpch_query):
    """ Run the given TPCH query after creating a BodoSQL context (entry point for sql query into pandas using Bodo)
    """
    customer_df, orders_df, lineitem_df, nation_df, region_df, supplier_df = load_tpch_data(dir_name)
    bc = bodosql.BodoSQLContext({"customer": customer_df,
                                 "orders": orders_df,
                                 "lineitem": lineitem_df,
                                 "nation": nation_df,
                                 "region": region_df,
                                 "supplier": supplier_df,
                                 })
    bodosql_out = bc.sql(tpch_query)
    # print output
    print(bodosql_out)
