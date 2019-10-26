package bench

const TPCHSQL = map[string]string {
	"q1":`select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date_sub('1998-12-01', interval 108 day)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
`,
}

// GenBenchSql gen bench sqls
func GenBenchSql() string {
	return TPCHSQL["q1"]
}