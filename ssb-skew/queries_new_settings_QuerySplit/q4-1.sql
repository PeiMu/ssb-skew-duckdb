
switch to c_r;
switch to relationshipcenter;

SET parallel_leader_participation = off;
set max_parallel_workers = '0';
set max_parallel_workers_per_gather = '0';
set shared_buffers = '512MB';
set temp_buffers = '2047MB';
set work_mem = '2047MB';
set effective_cache_size = '4 GB';
set statement_timeout = '1000s';
set default_statistics_target = 100;

SELECT D_YEAR, C_NATION, SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM DATE, CUSTOMER, SUPPLIER, PART, LINEORDER
WHERE LO_CUSTKEY = C_CUSTKEY
  AND LO_SUPPKEY = S_SUPPKEY
  AND LO_PARTKEY = P_PARTKEY
  AND LO_ORDERDATE = D_DATEKEY
  AND C_REGION = 'AMERICA'
  AND S_REGION = 'AMERICA'
  AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY D_YEAR, C_NATION
ORDER BY D_YEAR, C_NATION;
