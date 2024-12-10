
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

SELECT SUM(LO_REVENUE), D_YEAR, P_BRAND
FROM LINEORDER, DATE, PART, SUPPLIER
WHERE LO_ORDERDATE = D_DATEKEY
  AND LO_PARTKEY = P_PARTKEY
  AND LO_SUPPKEY = S_SUPPKEY
  AND P_CATEGORY = 'MFGR#12'
  AND S_REGION = 'AMERICA'
GROUP BY D_YEAR, P_BRAND
ORDER BY D_YEAR, P_BRAND;
