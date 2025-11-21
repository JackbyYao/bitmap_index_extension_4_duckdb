PRAGMA threads=8;
PRAGMA verify_parallelism;
LOAD 'bitmap_idx';
LOAD tpch;
CALL dbgen(sf=0.1);
CREATE INDEX C_MKTSEGMENT_idx ON CUSTOMER USING BITMAP (C_MKTSEGMENT);
CREATE INDEX O_ORDERPRIORITY_idx ON ORDERS USING BITMAP (O_ORDERPRIORITY);
CREATE INDEX O_ORDERSTATUS_idx ON ORDERS USING BITMAP (O_ORDERSTATUS);
CREATE INDEX S_NATIONKEY_idx ON SUPPLIER USING BITMAP (S_NATIONKEY);
CREATE INDEX P_TYPE_idx ON PART USING BITMAP (P_TYPE);
CREATE INDEX PS_SUPPKEY_idx ON PARTSUPP USING BITMAP (PS_SUPPKEY);
CREATE INDEX L_SUPPKEY_idx ON LINEITEM USING BITMAP (L_SUPPKEY);
CREATE INDEX L_RETURNFLAG_idx ON LINEITEM USING BITMAP (L_RETURNFLAG);

-- EXPLAIN
SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM (
    SELECT
        DATE_PART('year', o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year
ORDER BY o_year;
