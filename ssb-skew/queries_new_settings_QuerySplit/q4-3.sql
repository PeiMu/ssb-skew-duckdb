
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

SELECT D_YEAR, S_CITY, P_BRAND, SUM(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM DATE, CUSTOMER, SUPPLIER, PART, LINEORDER
WHERE LO_CUSTKEY = C_CUSTKEY
  AND LO_SUPPKEY = S_SUPPKEY
  AND LO_PARTKEY = P_PARTKEY
  AND LO_ORDERDATE = D_DATEKEY
  AND C_REGION = 'AMERICA'
  AND S_NATION = 'UNITED STATES'
  AND (D_YEAR = 1997 OR D_YEAR = 1998)
  AND P_CATEGORY = 'MFGR#14'
GROUP BY D_YEAR, S_CITY, P_BRAND
ORDER BY D_YEAR, S_CITY, P_BRAND;
