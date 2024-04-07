select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    count(*) as count_order
from
    TPCH_SF1.lineitem
where
    l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus