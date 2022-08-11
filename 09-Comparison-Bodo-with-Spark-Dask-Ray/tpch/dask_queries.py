"""

This code is adapted from
https://github.com/Bodo-inc/Bodo-examples/blob/master/examples/tpch/bodo_queries.py

The differences are in:
1. main(): how the data parser is setup to take parameters for Dask Client
2. run_queries(): checker to make sure Dask cluster is properly spawn and wait() to make sure data is all loaded into memory
3. individual queries: .compute() is added at the end to force code execution
4. Dask Pandas Gaps and Changes:
    a. pd.Timestamp("1995-01-01") → datetime.strptime("1995-01-01", '%Y-%m-%d')
    b. df.groupby([x,y,z],as_index=False) → df.groupby([x,y,z]).reset_index()
    c. df.NamedAgg() → do .agg() first, then rename with df.columns = [x,y,z]
    d. need to add “meta=(<column>, <dtype>)” to df.apply(func) → df.apply(func,meta=”…”)

Code is maintained by Zhuchang Zhan


dask package versions
    dask  2021.9.1
    distributed 2021.9.1
    dask-mpi 2.21.0
"""


import time
import argparse
import pandas
from datetime import datetime

import dask.dataframe as pd
from dask.distributed import Client, wait

import os,sys
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["AWS_DEFAULT_REGION"] = ""


def run_queries(data_folder,client,n_workers):

    t_start = time.time()

    # checking if all the workers have spawned
    if len(client.scheduler_info()["workers"]) < n_workers:
        while ((client.status == "running") and (len(client.scheduler_info()["workers"]) < n_workers)):
            print(len(client.scheduler_info()["workers"]))
            time.sleep(5.0)
        print("Cluster took %s seconds to fully spawn all workers"%(time.time()-t_start))

    # Load the data
    t1 = time.time()
    lineitem = load_lineitem(data_folder)
    orders = load_orders(data_folder)
    customer = load_customer(data_folder)
    nation = load_nation(data_folder)
    region = load_region(data_folder)
    supplier = load_supplier(data_folder)
    part = load_part(data_folder)
    partsupp = load_partsupp(data_folder)

    print("start data read")

    t0 = time.time()
    lineitem = client.persist(lineitem)
    wait(lineitem)
    print("loaded lineitem", time.time()-t0)

    t0 = time.time()
    orders = client.persist(orders)
    wait(orders)
    print("loaded orders", time.time()-t0)

    t0 = time.time()
    customer = client.persist(customer)
    wait(customer)
    print("loaded customer", time.time()-t0)

    t0 = time.time()
    nation = client.persist(nation)
    wait(nation)
    print("loaded nation", time.time()-t0)

    t0 = time.time()
    region = client.persist(region)
    wait(region)
    print("loaded region", time.time()-t0)

    t0 = time.time()
    supplier = client.persist(supplier)
    wait(supplier)
    print("loaded supplier", time.time()-t0)

    t0 = time.time()
    part = client.persist(part)
    wait(part)
    print("loaded part", time.time()-t0)

    t0 = time.time()
    partsupp = client.persist(partsupp)
    wait(partsupp)
    print("loaded partsupp", time.time()-t0)

    print("Reading time (s): ", time.time() - t1)

    t1 = time.time()
    # Run the Queries:
    # q01
    q01(lineitem)

    # q02
    q02(part, partsupp, supplier, nation, region)

    # q03
    q03(lineitem, orders, customer)

    # q04
    q04(lineitem, orders)

    # q05
    q05(lineitem, orders, customer, nation, region, supplier)

    # q06
    q06(lineitem)

    # q07
    q07(lineitem, supplier, orders, customer, nation)

    # q08
    q08(part, lineitem, supplier, orders, customer, nation, region)

    # q09
    q09(lineitem, orders, part, nation, partsupp, supplier)

    # q10
    q10(lineitem, orders, customer, nation)

    # q11
    q11(partsupp, supplier, nation)

    # q12
    q12(lineitem, orders)

    # q13
    q13(customer, orders)

    # q14
    q14(lineitem, part)

    # q15
    q15(lineitem, supplier)

    # q16
    q16(part, partsupp, supplier)

    # q17
    q17(lineitem, part)

    # q18
    q18(lineitem, orders, customer)

    # q19
    q19(lineitem, part)

    # q20
    q20(lineitem, part, nation, partsupp, supplier)

    # q21
    q21(lineitem, orders, supplier, nation)

    # q22
    q22(customer, orders)


    print("Total Query time (s): ", time.time() - t1)


def load_lineitem(data_folder):
    data_path = data_folder + "/lineitem.pq"
    df = pd.read_parquet(
        data_path,
    )
    df.L_SHIPDATE = pd.to_datetime(df.L_SHIPDATE, format='%Y-%m-%d')
    df.L_RECEIPTDATE = pd.to_datetime(df.L_RECEIPTDATE, format='%Y-%m-%d')
    df.L_COMMITDATE = pd.to_datetime(df.L_COMMITDATE, format='%Y-%m-%d')
    return df

def load_part(data_folder):
    data_path = data_folder + "/part.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def load_orders(data_folder):
    data_path = data_folder + "/orders.pq"
    df = pd.read_parquet(
        data_path,
    )
    df.O_ORDERDATE = pd.to_datetime(df.O_ORDERDATE, format='%Y-%m-%d')
    return df

def load_customer(data_folder):
    data_path = data_folder + "/customer.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def load_nation(data_folder):
    data_path = data_folder + "/nation.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def load_region(data_folder):
    data_path = data_folder + "/region.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def load_supplier(data_folder):
    data_path = data_folder + "/supplier.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def load_partsupp(data_folder):
    data_path = data_folder + "/partsupp.pq"
    df = pd.read_parquet(
        data_path,
    )
    return df

def q01(lineitem):

    t1 = time.time()
    date = datetime.strptime('1998-09-02', '%Y-%m-%d')
    lineitem_filtered = lineitem.loc[:, ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS",  "L_SHIPDATE", "L_ORDERKEY"]]
    sel = lineitem_filtered.L_SHIPDATE <= date
    lineitem_filtered = lineitem_filtered[sel].copy()
    lineitem_filtered["AVG_QTY"] = lineitem_filtered.L_QUANTITY
    lineitem_filtered["AVG_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE
    lineitem_filtered["DISC_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE * (1 - lineitem_filtered.L_DISCOUNT)
    lineitem_filtered["CHARGE"] = (
        lineitem_filtered.L_EXTENDEDPRICE * (1 - lineitem_filtered.L_DISCOUNT) * (1 + lineitem_filtered.L_TAX)
    )
    gb = lineitem_filtered.groupby(["L_RETURNFLAG", "L_LINESTATUS"])

    total = gb.agg(
        {
            "L_QUANTITY": "sum",
            "L_EXTENDEDPRICE": "sum",
            "DISC_PRICE": "sum",
            "CHARGE": "sum",
            "AVG_QTY": "mean",
            "AVG_PRICE": "mean",
            "L_DISCOUNT": "mean",
            "L_ORDERKEY": "count",
        }
    )

    total = total.compute().reset_index().sort_values(["L_RETURNFLAG", "L_LINESTATUS"])
    print(total)
    print("Q01 Execution time (s): ", time.time() - t1)

def q02(part, partsupp, supplier, nation, region):
    t1 = time.time()
    nation_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME", "N_REGIONKEY"]]
    region_filtered = region[(region["R_NAME"] == "EUROPE")]
    region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
    r_n_merged = nation_filtered.merge(region_filtered, left_on='N_REGIONKEY', right_on='R_REGIONKEY', how='inner')
    r_n_merged = r_n_merged.loc[:, ["N_NATIONKEY", "N_NAME"]]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]]
    s_r_n_merged = r_n_merged.merge(supplier_filtered, left_on="N_NATIONKEY", right_on="S_NATIONKEY", how="inner")
    s_r_n_merged = s_r_n_merged.loc[:, ["N_NAME", "S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]]
    partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY", "PS_SUPPLYCOST"]]
    ps_s_r_n_merged = s_r_n_merged.merge(partsupp_filtered, left_on="S_SUPPKEY", right_on="PS_SUPPKEY", how="inner")
    ps_s_r_n_merged = ps_s_r_n_merged.loc[:, ["N_NAME", "S_NAME", "S_ADDRESS", "S_PHONE", "S_ACCTBAL", "S_COMMENT", "PS_PARTKEY", "PS_SUPPLYCOST"]]
    part_filtered = part.loc[:, ["P_PARTKEY", "P_MFGR", "P_SIZE", "P_TYPE"]]
    part_filtered = part_filtered[(part_filtered["P_SIZE"] == 15) & (part_filtered["P_TYPE"].str.endswith("BRASS"))]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_MFGR"]]
    merged_df = part_filtered.merge(ps_s_r_n_merged, left_on='P_PARTKEY', right_on='PS_PARTKEY', how='inner')
    merged_df = merged_df.loc[:, ["N_NAME", "S_NAME", "S_ADDRESS", "S_PHONE", "S_ACCTBAL", "S_COMMENT", "PS_SUPPLYCOST", "P_PARTKEY", "P_MFGR"]]

    min_values = merged_df.groupby("P_PARTKEY")["PS_SUPPLYCOST"].min().reset_index()

    min_values.columns=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
    merged_df = merged_df.merge(min_values, left_on=["P_PARTKEY", "PS_SUPPLYCOST"], right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"], how="inner")
    total = merged_df.loc[:, ["S_ACCTBAL", "S_NAME", "N_NAME", "P_PARTKEY", "P_MFGR", "S_ADDRESS", "S_PHONE", "S_COMMENT"]]

    total = total.compute().sort_values(by=["S_ACCTBAL","N_NAME","S_NAME","P_PARTKEY",], ascending=[False,True,True,True,])

    print(total)
    print("Q02 Execution time (s): ", time.time() - t1)

def q03(lineitem, orders, customer):
    t1 = time.time()
    date = datetime.strptime('1995-03-04', '%Y-%m-%d')
    lineitem_filtered = lineitem.loc[:, ["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]]
    orders_filtered = orders.loc[:, ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    customer_filtered = customer.loc[:, ["C_MKTSEGMENT", "C_CUSTKEY"]]
    lsel = lineitem_filtered.L_SHIPDATE > date
    osel = orders_filtered.O_ORDERDATE < date
    csel = customer_filtered.C_MKTSEGMENT == "HOUSEHOLD"
    flineitem = lineitem_filtered[lsel]
    forders = orders_filtered[osel]
    fcustomer = customer_filtered[csel]
    jn1 = fcustomer.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn2 = jn1.merge(flineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    jn2["TMP"] = jn2.L_EXTENDEDPRICE * (1 - jn2.L_DISCOUNT)
    total = (
        jn2.groupby(["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"])[
            "TMP"
        ]
        .sum().compute().reset_index()
        .sort_values(["TMP"], ascending=False)
    )

    res = total.loc[:, ["L_ORDERKEY", "TMP", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    print(res.head(10))
    print("Q03 Execution time (s): ", time.time() - t1)

def q04(lineitem, orders):
    t1 = time.time()
    date1 = datetime.strptime("1993-11-01", '%Y-%m-%d')
    date2 = datetime.strptime("1993-08-01", '%Y-%m-%d')
    lsel = lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
    osel = (orders.O_ORDERDATE < date1) & (orders.O_ORDERDATE >= date2)
    flineitem = lineitem[lsel]
    forders = orders[osel]
    forders = forders[["O_ORDERKEY","O_ORDERPRIORITY"]]
    #jn = forders[forders["O_ORDERKEY"].compute().isin(flineitem["L_ORDERKEY"])] # doesn't support isin
    jn = forders.merge(flineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY").drop_duplicates(subset=["O_ORDERKEY"])[["O_ORDERPRIORITY","O_ORDERKEY"]]
    total = jn.groupby("O_ORDERPRIORITY")["O_ORDERKEY"].count().reset_index().sort_values(["O_ORDERPRIORITY"])
    print(total.compute())
    print("Q04 Execution time (s): ", time.time() - t1)

def q05(lineitem, orders, customer, nation, region, supplier):
    t1 = time.time()
    date1 = datetime.strptime("1996-01-01", '%Y-%m-%d')
    date2 = datetime.strptime("1997-01-01", '%Y-%m-%d')

    rsel = region.R_NAME == "ASIA"
    osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)


    forders = orders[osel]
    fregion = region[rsel]
    jn1 = fregion.merge(nation, left_on="R_REGIONKEY", right_on="N_REGIONKEY")
    jn2 = jn1.merge(customer, left_on="N_NATIONKEY", right_on="C_NATIONKEY")
    jn3 = jn2.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn4 = jn3.merge(lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

    jn5 = supplier.merge(
        jn4, left_on=["S_SUPPKEY", "S_NATIONKEY"], right_on=["L_SUPPKEY", "N_NATIONKEY"]
    )
    jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1.0 - jn5.L_DISCOUNT)
    gb = jn5.groupby("N_NAME")["TMP"].sum()

    total = gb.compute().reset_index().sort_values("TMP", ascending=False)
    print(total)
    print("Q05 Execution time (s): ", time.time() - t1)

def q06(lineitem):
    t1 = time.time()
    date1 = datetime.strptime("1996-01-01", '%Y-%m-%d')
    date2 = datetime.strptime("1997-01-01", '%Y-%m-%d')

    lineitem_filtered = lineitem.loc[:, ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]]
    sel = (
        (lineitem_filtered.L_SHIPDATE >= date1)
        & (lineitem_filtered.L_SHIPDATE < date2)
        & (lineitem_filtered.L_DISCOUNT >= 0.08)
        & (lineitem_filtered.L_DISCOUNT <= 0.1)
        & (lineitem_filtered.L_QUANTITY < 24)
    )
    flineitem = lineitem_filtered[sel]
    total = (flineitem.L_EXTENDEDPRICE * flineitem.L_DISCOUNT).sum()
    print(total.compute())
    print("Q06 Execution time (s): ", time.time() - t1)

def q07(lineitem, supplier, orders, customer, nation):
    """ This version is faster than q07_old. Keeping the old one for reference """
    t1 = time.time()

    lineitem_filtered = lineitem[(lineitem["L_SHIPDATE"] >= datetime.strptime("1995-01-01", '%Y-%m-%d')) &
                                 (lineitem["L_SHIPDATE"] < datetime.strptime("1997-01-01", '%Y-%m-%d'))]
    lineitem_filtered["L_YEAR"] = lineitem_filtered["L_SHIPDATE"].apply(lambda x: x.year)
    lineitem_filtered["VOLUME"] = lineitem_filtered["L_EXTENDEDPRICE"] * (1.0 - lineitem_filtered["L_DISCOUNT"])
    lineitem_filtered = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY", "L_YEAR", "VOLUME"]]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    orders_filtered = orders.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
    customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
    n1 = nation[(nation["N_NAME"] == "FRANCE")].loc[:, ["N_NATIONKEY", "N_NAME"]]
    n2 = nation[(nation["N_NAME"] == "GERMANY")].loc[:, ["N_NATIONKEY", "N_NAME"]]

    # ----- do nation 1 -----
    N1_C = customer_filtered.merge(n1, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N1_C = N1_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "CUST_NATION"})
    N1_C_O = N1_C.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')
    N1_C_O = N1_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

    N2_S = supplier_filtered.merge(n2, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N2_S = N2_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "SUPP_NATION"})
    N2_S_L = N2_S.merge(lineitem_filtered, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')
    N2_S_L = N2_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

    total1 = N1_C_O.merge(N2_S_L, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    total1 = total1.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

    # ----- do nation 2 ----- (same as nation 1 section but with nation 2)
    N2_C = customer_filtered.merge(n2, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N2_C = N2_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "CUST_NATION"})
    N2_C_O = N2_C.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')
    N2_C_O = N2_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

    N1_S = supplier_filtered.merge(n1, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    N1_S = N1_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(columns={"N_NAME": "SUPP_NATION"})
    N1_S_L = N1_S.merge(lineitem_filtered, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')
    N1_S_L = N1_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

    total2 = N2_C_O.merge(N1_S_L, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    total2 = total2.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

    # concat results
    total = pd.concat([total1, total2])
    #total = total.groupby(["SUPP_NATION", "CUST_NATION", "L_YEAR"]).agg(REVENUE=pd.NamedAgg(column="VOLUME", aggfunc="sum"))
    total = total.groupby(["SUPP_NATION", "CUST_NATION", "L_YEAR"]).VOLUME.agg("sum")
    total.columns = ["SUPP_NATION", "CUST_NATION", "L_YEAR", "REVENUE"]

    total = total.compute().reset_index().sort_values(by=["SUPP_NATION","CUST_NATION","L_YEAR"], ascending=[True,True,True,])
    print(total)
    print("Q07 Execution time (s): ", time.time() - t1)

def q08(part, lineitem, supplier, orders, customer, nation, region):
    t1 = time.time()
    part_filtered = part[(part["P_TYPE"] == "ECONOMY ANODIZED STEEL")]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY"]]
    lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_SUPPKEY", "L_ORDERKEY"]]
    lineitem_filtered["VOLUME"] = lineitem["L_EXTENDEDPRICE"] * (1.0 - lineitem["L_DISCOUNT"])
    total = part_filtered.merge(lineitem_filtered, left_on="P_PARTKEY", right_on="L_PARTKEY", how="inner")
    total = total.loc[:, ["L_SUPPKEY", "L_ORDERKEY", "VOLUME"]]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    total = total.merge(supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner")
    total = total.loc[:, ["L_ORDERKEY", "VOLUME", "S_NATIONKEY"]]
    orders_filtered = orders[(orders["O_ORDERDATE"] >= datetime.strptime("1995-01-01", '%Y-%m-%d')) &
                             (orders["O_ORDERDATE"] < datetime.strptime("1997-01-01", '%Y-%m-%d'))]
    orders_filtered["O_YEAR"] = orders_filtered["O_ORDERDATE"].apply(lambda x: x.year)
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY", "O_YEAR"]]
    total = total.merge(orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner")
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_CUSTKEY", "O_YEAR"]]
    customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
    total = total.merge(customer_filtered, left_on="O_CUSTKEY", right_on="C_CUSTKEY", how="inner")
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "C_NATIONKEY"]]
    n1_filtered = nation.loc[:, ["N_NATIONKEY", "N_REGIONKEY"]]
    n2_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME"]].rename(columns={"N_NAME": "NATION"})
    total = total.merge(n1_filtered, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner")
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "N_REGIONKEY"]]
    total = total.merge(n2_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner")
    total = total.loc[:, ["VOLUME", "O_YEAR", "N_REGIONKEY", "NATION"]]
    region_filtered = region[(region["R_NAME"] == "AMERICA")]
    region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
    total = total.merge(region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner")
    total = total.loc[:, ["VOLUME", "O_YEAR", "NATION"]]

    def udf(df):
        demonimator = df["VOLUME"].sum()
        df = df[df["NATION"] == "BRAZIL"]
        numerator = df["VOLUME"].sum()
        return numerator / demonimator


    total = total.groupby("O_YEAR").apply(udf)
    total = total.compute().reset_index().sort_values(by=["O_YEAR",], ascending=[True,])
    total.columns = ["O_YEAR", "MKT_SHARE"]
    print(total)
    print("Q08 Execution time (s): ", time.time() - t1)

def q09(lineitem, orders, part, nation, partsupp, supplier):
    t1 = time.time()
    psel = part.P_NAME.str.contains("ghost")
    fpart = part[psel]
    jn1 = lineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jn2 = jn1.merge(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
    jn3 = jn2.merge(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
    jn4 = partsupp.merge(
        jn3, left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"]
    )
    jn5 = jn4.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1 - jn5.L_DISCOUNT) - (
        (1 * jn5.PS_SUPPLYCOST) * jn5.L_QUANTITY
    )
    jn5["O_YEAR"] = jn5.O_ORDERDATE.apply(lambda x: x.year)
    gb = jn5.groupby(["N_NAME", "O_YEAR"])["TMP"].sum()
    total = gb.compute().reset_index().sort_values(["N_NAME", "O_YEAR"], ascending=[True, False])
    print(total)
    print("Q09 Execution time (s): ", time.time() - t1)

def q10(lineitem, orders, customer, nation):
    t1 = time.time()
    date1 = datetime.strptime("1994-11-01", '%Y-%m-%d')
    date2 = datetime.strptime("1995-02-01", '%Y-%m-%d')
    osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)
    lsel = lineitem.L_RETURNFLAG == "R"
    forders = orders[osel]
    flineitem = lineitem[lsel]
    jn1 = flineitem.merge(forders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
    jn3 = jn2.merge(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
    jn3["TMP"] = jn3.L_EXTENDEDPRICE * (1.0 - jn3.L_DISCOUNT)
    gb = jn3.groupby(
        [
            "C_CUSTKEY",
            "C_NAME",
            "C_ACCTBAL",
            "C_PHONE",
            "N_NAME",
            "C_ADDRESS",
            "C_COMMENT",
        ],
    )["TMP"].sum()
    total = gb.compute().reset_index().sort_values("TMP", ascending=False)
    print(total.head(20))
    print(total.shape)
    print("Q10 Execution time (s): ", time.time() - t1)

def q11(partsupp, supplier, nation):
    t1 = time.time()
    partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY"]]
    partsupp_filtered["TOTAL_COST"] = partsupp["PS_SUPPLYCOST"] * partsupp["PS_AVAILQTY"]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    ps_supp_merge = partsupp_filtered.merge(supplier_filtered, left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='inner')
    ps_supp_merge.loc[:, ["PS_PARTKEY", "S_NATIONKEY", "TOTAL_COST"]]
    nation_filtered = nation[(nation["N_NAME"] == "GERMANY")]
    nation_filtered = nation_filtered.loc[:, ["N_NATIONKEY"]]
    ps_supp_n_merge = ps_supp_merge.merge(nation_filtered, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    ps_supp_n_merge = ps_supp_n_merge.loc[:, ["PS_PARTKEY", "TOTAL_COST"]]
    sum_val = ps_supp_n_merge["TOTAL_COST"].sum() * 0.0001


    total = ps_supp_n_merge.groupby(["PS_PARTKEY"]).TOTAL_COST.agg("sum").reset_index()
    total = total.rename(columns={'TOTAL_COST':'VALUE'})
    total = total[total["VALUE"] > sum_val]
    total = total.compute().sort_values("VALUE", ascending=False)
    print(total)
    print("Q11 Execution time (s): ", time.time() - t1)

def q12(lineitem, orders):
    t1 = time.time()
    date1 = datetime.strptime("1994-01-01", '%Y-%m-%d')
    date2 = datetime.strptime("1995-01-01", '%Y-%m-%d')
    sel = (
        (lineitem.L_RECEIPTDATE < date2)
        & (lineitem.L_COMMITDATE < date2)
        & (lineitem.L_SHIPDATE < date2)
        & (lineitem.L_SHIPDATE < lineitem.L_COMMITDATE)
        & (lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE)
        & (lineitem.L_RECEIPTDATE >= date1)
        & ((lineitem.L_SHIPMODE == "MAIL") | (lineitem.L_SHIPMODE == "SHIP"))
    )
    flineitem = lineitem[sel]
    jn = flineitem.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    gb = jn.groupby("L_SHIPMODE")["O_ORDERPRIORITY"]

    def g1(x):
        return x.apply(lambda s: ((s == "1-URGENT") | (s == "2-HIGH")).sum())

    def g2(x):
        return x.apply(lambda s: ((s != "1-URGENT") & (s != "2-HIGH")).sum())

    g1_agg = pd.Aggregation('g1', g1, lambda s0: s0.sum())
    g2_agg = pd.Aggregation('g2', g2, lambda s0: s0.sum())
    total = gb.agg([g1_agg, g2_agg])
    total = total.compute().reset_index().sort_values("L_SHIPMODE")
    print(total)
    print("Q12 Execution time (s): ", time.time() - t1)

def q13(customer, orders):
    t1 = time.time()
    customer_filtered = customer.loc[:, ["C_CUSTKEY"]]
    orders_filtered = orders[~orders["O_COMMENT"].str.contains("special(\S|\s)*requests")]
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
    c_o_merged = customer_filtered.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
    c_o_merged = c_o_merged.loc[:, ["C_CUSTKEY", "O_ORDERKEY"]]

    count_df = c_o_merged.groupby(["C_CUSTKEY"]).O_ORDERKEY.agg("count").reset_index()
    count_df = count_df.rename(columns={'O_ORDERKEY':'C_COUNT'})

    total = count_df.groupby(["C_COUNT"]).size().reset_index()
    total.columns = ["C_COUNT","CUSTDIST"]
    total = total.compute().sort_values(by=["CUSTDIST","C_COUNT"], ascending=[False,False,])
    print(total)
    print("Q13 Execution time (s): ", time.time() - t1)

def q14(lineitem, part):
    t1 = time.time()
    startDate = datetime.strptime("1994-03-01", '%Y-%m-%d')
    endDate = datetime.strptime("1994-04-01", '%Y-%m-%d')
    p_type_like = "PROMO"
    part_filtered = part.loc[:, ["P_PARTKEY", "P_TYPE"]]
    lineitem_filtered = lineitem.loc[:, ["L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE", "L_PARTKEY"]]
    sel = (lineitem_filtered.L_SHIPDATE >= startDate) & (lineitem_filtered.L_SHIPDATE < endDate)
    flineitem = lineitem_filtered[sel]
    jn = flineitem.merge(part_filtered, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jn["TMP"] = jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)
    total = jn[jn.P_TYPE.str.startswith(p_type_like)].TMP.sum() * 100 / jn.TMP.sum()
    print(total.compute())
    print("Q14 Execution time (s): ", time.time() - t1)

def q15(lineitem, supplier):
    t1 = time.time()
    lineitem_filtered = lineitem[(lineitem["L_SHIPDATE"] >= datetime.strptime("1996-01-01", '%Y-%m-%d')) & (lineitem["L_SHIPDATE"] < (datetime.strptime("1996-04-01", '%Y-%m-%d')))]# + pd.DateOffset(months=3)))]
    lineitem_filtered["REVENUE_PARTS"] = lineitem_filtered["L_EXTENDEDPRICE"] * (1.0 - lineitem_filtered["L_DISCOUNT"])
    lineitem_filtered = lineitem_filtered.loc[:, ["L_SUPPKEY", "REVENUE_PARTS"]]
    revenue_table = lineitem_filtered.groupby("L_SUPPKEY")["REVENUE_PARTS"].agg("sum").reset_index()
    revenue_table = revenue_table.rename(columns={"REVENUE_PARTS":"TOTAL_REVENUE","L_SUPPKEY": "SUPPLIER_NO"})
    max_revenue = revenue_table["TOTAL_REVENUE"].max()
    revenue_table = revenue_table[revenue_table["TOTAL_REVENUE"] == max_revenue]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE"]]
    total = supplier_filtered.merge(revenue_table, left_on="S_SUPPKEY", right_on="SUPPLIER_NO", how="inner")
    total = total.loc[:, ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"]]
    print(total.compute())
    print("Q15 Execution time (s): ", time.time() - t1)

def q16(part, partsupp, supplier):
    t1 = time.time()
    part_filtered = part[
                      (part["P_BRAND"] != "Brand#45")
                      & (~part["P_TYPE"].str.contains("^MEDIUM POLISHED"))
                      & part["P_SIZE"].isin([49, 14, 23, 45, 19, 3, 36, 9])
                    ]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_BRAND", "P_TYPE", "P_SIZE"]]
    partsupp_filtered = partsupp.loc[:, ["PS_PARTKEY", "PS_SUPPKEY"]]
    total = part_filtered.merge(partsupp_filtered, left_on="P_PARTKEY", right_on="PS_PARTKEY", how="inner")
    total = total.loc[:, ["P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY"]]
    supplier_filtered = supplier[supplier["S_COMMENT"].str.contains("Customer(\S|\s)*Complaints")]
    supplier_filtered = supplier_filtered.loc[:, ["S_SUPPKEY"]].drop_duplicates()
    # left merge to select only ps_suppkey values not in supplier_filtered
    total = total.merge(supplier_filtered, left_on="PS_SUPPKEY", right_on="S_SUPPKEY", how="left")
    total = total[total["S_SUPPKEY"].isna()]
    total = total.loc[:, ["P_BRAND", "P_TYPE", "P_SIZE", "PS_SUPPKEY"]]
    total = total.groupby(["P_BRAND", "P_TYPE", "P_SIZE"])["PS_SUPPKEY"].nunique().reset_index()
    total.columns = ["P_BRAND", "P_TYPE", "P_SIZE", "SUPPLIER_CNT"]
    total = total.compute().sort_values(by=["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"], ascending=[False, True, True, True])
    print(total)
    print("Q16 Execution time (s): ", time.time() - t1)

def q17(lineitem, part):
    t1 = time.time()
    left = lineitem.loc[:, ["L_PARTKEY", "L_QUANTITY", "L_EXTENDEDPRICE"]]
    right = part[((part["P_BRAND"] == "Brand#23") & (part["P_CONTAINER"] == "MED BOX"))]
    right = right.loc[:, ["P_PARTKEY"]]
    line_part_merge = left.merge(right, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
    line_part_merge = line_part_merge.loc[:, ["L_QUANTITY", "L_EXTENDEDPRICE", "P_PARTKEY"]]
    lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_QUANTITY"]]
    lineitem_avg = lineitem_filtered.groupby(["L_PARTKEY"]).L_QUANTITY.agg("mean").reset_index().rename(columns={'L_QUANTITY':'avg'})
    lineitem_avg["avg"] = 0.2 * lineitem_avg["avg"]
    lineitem_avg = lineitem_avg.loc[:, ["L_PARTKEY", "avg"]]
    total = line_part_merge.merge(lineitem_avg, left_on='P_PARTKEY', right_on='L_PARTKEY', how='inner')
    total = total[total["L_QUANTITY"] < total["avg"]]
    total = pandas.DataFrame({"avg_yearly": [(total["L_EXTENDEDPRICE"].sum() / 7.0).compute()]})
    print(total)
    print("Q17 Execution time (s): ", time.time() - t1)

def q18(lineitem, orders, customer):
    t1 = time.time()
    gb1 = lineitem.groupby("L_ORDERKEY")["L_QUANTITY"].sum().reset_index()
    fgb1 = gb1[gb1.L_QUANTITY > 300]
    jn1 = fgb1.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
    gb2 = jn2.groupby(
        ["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"],
    )["L_QUANTITY"].sum()
    total = gb2.compute().reset_index().sort_values(["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True])
    print(total.head(100))
    print("Q18 Execution time (s): ", time.time() - t1)

def q19(lineitem, part):
    t1 = time.time()
    Brand31 = "Brand#31"
    Brand43 = "Brand#43"
    SMBOX = "SM BOX"
    SMCASE = "SM CASE"
    SMPACK = "SM PACK"
    SMPKG = "SM PKG"
    MEDBAG = "MED BAG"
    MEDBOX = "MED BOX"
    MEDPACK = "MED PACK"
    MEDPKG = "MED PKG"
    LGBOX = "LG BOX"
    LGCASE = "LG CASE"
    LGPACK = "LG PACK"
    LGPKG = "LG PKG"
    DELIVERINPERSON = "DELIVER IN PERSON"
    AIR = "AIR"
    AIRREG = "AIRREG"
    lsel = (
        (
            ((lineitem.L_QUANTITY <= 36) & (lineitem.L_QUANTITY >= 26))
            | ((lineitem.L_QUANTITY <= 25) & (lineitem.L_QUANTITY >= 15))
            | ((lineitem.L_QUANTITY <= 14) & (lineitem.L_QUANTITY >= 4))
        )
        & (lineitem.L_SHIPINSTRUCT == DELIVERINPERSON)
        & ((lineitem.L_SHIPMODE == AIR) | (lineitem.L_SHIPMODE == AIRREG))
    )
    psel = (part.P_SIZE >= 1) & (
        (
            (part.P_SIZE <= 5)
            & (part.P_BRAND == Brand31)
            & (
                (part.P_CONTAINER == SMBOX)
                | (part.P_CONTAINER == SMCASE)
                | (part.P_CONTAINER == SMPACK)
                | (part.P_CONTAINER == SMPKG)
            )
        )
        | (
            (part.P_SIZE <= 10)
            & (part.P_BRAND == Brand43)
            & (
                (part.P_CONTAINER == MEDBAG)
                | (part.P_CONTAINER == MEDBOX)
                | (part.P_CONTAINER == MEDPACK)
                | (part.P_CONTAINER == MEDPKG)
            )
        )
        | (
            (part.P_SIZE <= 15)
            & (part.P_BRAND == Brand43)
            & (
                (part.P_CONTAINER == LGBOX)
                | (part.P_CONTAINER == LGCASE)
                | (part.P_CONTAINER == LGPACK)
                | (part.P_CONTAINER == LGPKG)
            )
        )
    )
    flineitem = lineitem[lsel]
    fpart = part[psel]
    jn = flineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jnsel = (
        (
            (jn.P_BRAND == Brand31)
            & (
                (jn.P_CONTAINER == SMBOX)
                | (jn.P_CONTAINER == SMCASE)
                | (jn.P_CONTAINER == SMPACK)
                | (jn.P_CONTAINER == SMPKG)
            )
            & (jn.L_QUANTITY >= 4)
            & (jn.L_QUANTITY <= 14)
            & (jn.P_SIZE <= 5)
        ) | (
            (jn.P_BRAND == Brand43)
            & (
                (jn.P_CONTAINER == MEDBAG)
                | (jn.P_CONTAINER == MEDBOX)
                | (jn.P_CONTAINER == MEDPACK)
                | (jn.P_CONTAINER == MEDPKG)
            )
            & (jn.L_QUANTITY >= 15)
            & (jn.L_QUANTITY <= 25)
            & (jn.P_SIZE <= 10)
        ) | (
            (jn.P_BRAND == Brand43)
            & (
                (jn.P_CONTAINER == LGBOX)
                | (jn.P_CONTAINER == LGCASE)
                | (jn.P_CONTAINER == LGPACK)
                | (jn.P_CONTAINER == LGPKG)
            )
            & (jn.L_QUANTITY >= 26)
            & (jn.L_QUANTITY <= 36)
            & (jn.P_SIZE <= 15)
        )
    )
    jn = jn[jnsel]
    total = (jn.L_EXTENDEDPRICE * (1.0 - jn.L_DISCOUNT)).sum()
    print(total.compute())
    print("Q19 Execution time (s): ", time.time() - t1)

def q20(lineitem, part, nation, partsupp, supplier):
    t1 = time.time()
    date1 = datetime.strptime("1996-01-01", '%Y-%m-%d')
    date2 = datetime.strptime("1997-01-01", '%Y-%m-%d')
    psel = part.P_NAME.str.startswith("azure")
    nsel = nation.N_NAME == "JORDAN"
    lsel = (lineitem.L_SHIPDATE >= date1) & (lineitem.L_SHIPDATE < date2)
    fpart = part[psel]
    fnation = nation[nsel]
    flineitem = lineitem[lsel]
    jn1 = fpart.merge(partsupp, left_on="P_PARTKEY", right_on="PS_PARTKEY")
    jn2 = jn1.merge(
        flineitem,
        left_on=["PS_PARTKEY", "PS_SUPPKEY"],
        right_on=["L_PARTKEY", "L_SUPPKEY"],
    )
    gb = jn2.groupby(["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY"])[
        "L_QUANTITY"
    ].sum().reset_index()
    gbsel = gb.PS_AVAILQTY > (0.5 * gb.L_QUANTITY)
    fgb = gb[gbsel]
    jn3 = fgb.merge(supplier, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
    jn4 = fnation.merge(jn3, left_on="N_NATIONKEY", right_on="S_NATIONKEY")
    jn4 = jn4.loc[:, ["S_NAME", "S_ADDRESS"]]
    total = jn4.compute().sort_values("S_NAME").drop_duplicates()
    print(total)
    print("Q20 Execution time (s): ", time.time() - t1)

def q21(lineitem, orders, supplier, nation):
    t1 = time.time()
    lineitem_filtered = lineitem.loc[:, ["L_ORDERKEY", "L_SUPPKEY", "L_RECEIPTDATE", "L_COMMITDATE"]]

    # Exists
    lineitem_orderkeys = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY"]].groupby("L_ORDERKEY")["L_SUPPKEY"].nunique().reset_index()
    lineitem_orderkeys.columns = ["L_ORDERKEY", "nunique_col"]
    lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] > 1]
    lineitem_orderkeys = lineitem_orderkeys.loc[:, ["L_ORDERKEY"]]

    # Filter
    lineitem_filtered = lineitem_filtered[lineitem_filtered["L_RECEIPTDATE"] > lineitem_filtered["L_COMMITDATE"]]
    lineitem_filtered = lineitem_filtered.loc[:, ["L_ORDERKEY", "L_SUPPKEY"]]

    # Merge Filter + Exists
    lineitem_filtered = lineitem_filtered.merge(lineitem_orderkeys, on="L_ORDERKEY", how="inner")

    # Not Exists: Check the exists condition isn't still satisfied on the output.
    lineitem_orderkeys = lineitem_filtered.groupby("L_ORDERKEY")["L_SUPPKEY"].nunique().reset_index()
    lineitem_orderkeys.columns = ["L_ORDERKEY", "nunique_col"]
    lineitem_orderkeys = lineitem_orderkeys[lineitem_orderkeys["nunique_col"] == 1]
    lineitem_orderkeys = lineitem_orderkeys.loc[:, ["L_ORDERKEY"]]

    # Merge Filter + Not Exists
    lineitem_filtered = lineitem_filtered.merge(lineitem_orderkeys, on="L_ORDERKEY", how="inner")

    orders_filtered = orders.loc[:, ["O_ORDERSTATUS", "O_ORDERKEY"]]
    orders_filtered = orders_filtered[orders_filtered["O_ORDERSTATUS"] == "F"]
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY"]]
    total = lineitem_filtered.merge(orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner")
    total = total.loc[:, ["L_SUPPKEY"]]

    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY", "S_NAME"]]
    total = total.merge(supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner")
    total = total.loc[:, ["S_NATIONKEY", "S_NAME"]]
    nation_filtered = nation.loc[:, ["N_NAME", "N_NATIONKEY"]]
    nation_filtered = nation_filtered[nation_filtered["N_NAME"] == "SAUDI ARABIA"]
    total = total.merge(nation_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner")
    total = total.loc[:, ["S_NAME"]]
    total = total.groupby("S_NAME").size().reset_index()
    total.columns = ["S_NAME", "NUMWAIT"]
    total = total.compute().sort_values(by=["NUMWAIT","S_NAME",], ascending=[False,True,])
    print(total)
    print("Q21 Execution time (s): ", time.time() - t1)

def q22(customer, orders):
    t1 = time.time()
    customer_filtered = customer.loc[:, ["C_ACCTBAL", "C_CUSTKEY"]]
    customer_filtered["CNTRYCODE"] = customer["C_PHONE"].str.slice(0, 2)
    customer_filtered = customer_filtered[(customer["C_ACCTBAL"] > 0.00) & customer_filtered["CNTRYCODE"].isin(["13", "31", "23", "29", "30", "18", "17"])]
    avg_value = customer_filtered["C_ACCTBAL"].mean()
    customer_filtered = customer_filtered[customer_filtered["C_ACCTBAL"] > avg_value]
    # Select only the keys that don't match by performing a left join and only selecting columns with an na value
    orders_filtered = orders.loc[:, ["O_CUSTKEY"]].drop_duplicates()
    customer_keys = customer_filtered.loc[:, ["C_CUSTKEY"]].drop_duplicates()
    customer_selected = customer_keys.merge(orders_filtered, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')
    customer_selected= customer_selected[customer_selected["O_CUSTKEY"].isna()]
    customer_selected = customer_selected.loc[:, ["C_CUSTKEY"]]
    customer_selected = customer_selected.merge(customer_filtered, on="C_CUSTKEY", how="inner")
    customer_selected = customer_selected.loc[:, ["CNTRYCODE", "C_ACCTBAL"]]
    agg1 = customer_selected.groupby(["CNTRYCODE"]).size().reset_index()
    agg1.columns = ["CNTRYCODE", "NUMCUST"]
    agg2 = customer_selected.groupby(["CNTRYCODE"]).C_ACCTBAL.agg("sum").reset_index()
    agg2 = agg2.rename(columns={'C_ACCTBAL':'TOTACCTBAL'})
    total = agg1.merge(agg2, on="CNTRYCODE", how="inner")
    total = total.compute().sort_values(by=["CNTRYCODE",], ascending=[True,])
    print(total)
    print("Q22 Execution time (s): ", time.time() - t1)

def main():
    parser = argparse.ArgumentParser(description="Bodo tpch-queries")
    parser.add_argument(
        "--folder",
        type=str,
        help="The folder containing TPCH data",
    )
    parser.add_argument(
        "--worker",
        required=False,
        type=int,
        default=1,
        help="minimum number of workers required before running dask, should be smaller than max cores",
    )
    parser.add_argument(
        "--scheduler-file",
        type=str,
        default="~/scheduler.json",
        help="The path to the scheduler file",
    )

    args = parser.parse_args()
    folder = args.folder
    n_workers = args.worker
    client = Client(scheduler_file=args.scheduler_file)
    print(client)

    if "s3" in folder and os.environ["AWS_ACCESS_KEY_ID"] == "":
        print("Please update your AWS credentials")
        sys.exit()

    run_queries(folder,client,n_workers)


if __name__ == "__main__":
    main()
