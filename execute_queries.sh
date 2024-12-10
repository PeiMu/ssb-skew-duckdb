dir="/home/pei/Project/benchmarks/ssb-skew-duckdb/ssb-skew/queries"
iteration=1

rm -f result/*
mkdir -p result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a queries_results_${i}.txt;
    psql -U postgres -d postgres -f "${sql}" 2>&1|tee -a queries_results_${i}.txt;
  done
done

mv queries_results_* result/.
