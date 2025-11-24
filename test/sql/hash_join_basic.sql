-- Hash Join Test Case with Bitmap Index Optimization
-- This test case is designed to test bitmap index optimization on hash join

LOAD 'bitmap_idx';
INSTALL './build/release/extension/bitmap_idx/bitmap_idx.duckdb_extension';

-- Ensure hash join is used (disable range joins)
PRAGMA prefer_range_joins = false;
-- Optional: speed up testing
-- PRAGMA enable_verification = false;


-- Create test tables with sufficient size to avoid nested loop join
-- Table t1: 10,000 rows with keys 0-99 (100 distinct keys)
CREATE TABLE t1 AS 
SELECT i as id, i % 100 as key, 'value_' || i::VARCHAR as data 
FROM range(0, 10000) t(i);

-- Table t2: 50,000 rows with keys 0-99 (100 distinct keys, many duplicates)
CREATE TABLE t2 AS 
SELECT i as id, i % 100 as key, 'other_' || i::VARCHAR as data 
FROM range(0, 50000) t(i);

-- Create bitmap indexes on join keys
CREATE INDEX idx_t1_key ON t1 USING BITMAP(key);
CREATE INDEX idx_t2_key ON t2 USING BITMAP(key);

-- Test Case 1: Both tables have bitmap index
-- Should use BitmapIndexJoin with build on smaller table (t1)
EXPLAIN ANALYZE 
SELECT t1.id, t1.data, t2.data 
FROM t1 INNER JOIN t2 ON t1.key = t2.key 
-- LIMIT 201
;

-- Test Case 2: Only one table has bitmap index
-- Drop index on t2
DROP INDEX idx_t2_key;

-- Should use BitmapIndexJoin with build on t2 (no index), probe on t1 (has index)
EXPLAIN ANALYZE 
SELECT t1.id, t1.data, t2.data 
FROM t1 INNER JOIN t2 ON t1.key = t2.key 
-- LIMIT 201
;

-- Test Case 3: Neither table has bitmap index
-- Drop index on t1
DROP INDEX idx_t1_key;

-- Should use regular HashJoin (no optimization)
EXPLAIN ANALYZE 
SELECT t1.id, t1.data, t2.data 
FROM t1 INNER JOIN t2 ON t1.key = t2.key 
-- LIMIT 201
;

-- Optional: Use EXPLAIN ANALYZE to see actual execution statistics
-- EXPLAIN ANALYZE SELECT t1.id, t1.data, t2.data 
-- FROM t1 INNER JOIN t2 ON t1.key = t2.key 
-- LIMIT 100;

-- Actual query execution (without LIMIT for full join)
-- SELECT t1.id, t1.data, t2.data 
-- FROM t1 INNER JOIN t2 ON t1.key = t2.key;

