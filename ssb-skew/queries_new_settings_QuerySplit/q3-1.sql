
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

SELECT C_NATION, S_NATION, D_YEAR, SUM(LO_REVENUE) AS revenue
FROM CUSTOMER, LINEORDER, SUPPLIER, DATE
WHERE LO_CUSTKEY = C_CUSTKEY
  AND LO_SUPPKEY = S_SUPPKEY
  AND LO_ORDERDATE = D_DATEKEY
  AND C_REGION = 'ASIA'
  AND S_REGION = 'ASIA'
  AND D_YEAR >= 1992
  AND D_YEAR <= 1997
GROUP BY C_NATION, S_NATION, D_YEAR
ORDER BY D_YEAR ASC, revenue DESC;
