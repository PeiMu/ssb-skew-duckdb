#!/usr/bin/env python3

import glob
import os
import psycopg2
import subprocess as sp
import time

nruns = 5
threads = ["1"]
# todo: include "imdb"
benchmarks = ["ssb", "ssb-skew"]
cwd = os.getcwd()

check_explain_analyze = False

def extract_join_intermediates(qp):
    counter = 0
    if qp["Node Type"] == "Nested Loop" or qp["Node Type"] == "Hash Join" or qp["Node Type"] == "Merge Join":
        counter += qp["Actual Rows"] * qp["Actual Loops"]
    if "Plans" in qp:
        for plan in qp["Plans"]:
            counter += extract_join_intermediates(plan)
    return counter


# Run Postgres
def run_postgres(query_split=False):
    # todo: create db and user
    pg_con = psycopg2.connect(user="postgres", host="127.0.0.1", port=5432)
    cur = pg_con.cursor()

    for benchmark in benchmarks:
        sp.call(["mkdir", "-p", f"{cwd}/experiment-results/4_1_endtoend/{benchmark}/postgres"])
        print(f"Loading {benchmark} data...")
        cur.execute(open(f"{cwd}/schema-{benchmark}.sql", "r").read())
        tables = glob.glob(os.path.join(f"{cwd}/postgres_data/{benchmark}", "*.tbl"))
        tables.sort()
        for table in tables:
            with open(table, "r") as tbl_file:
                tbl_name = table.split("/")[-1].split(".")[0]
                # skip the title bar
                next(tbl_file)
                cur.copy_expert(f"COPY {tbl_name} FROM STDIN ( FORMAT CSV, DELIMITER '|' )", tbl_file)
                cur.execute("commit;")
        cur.execute(open(f"{cwd}/fkeys-{benchmark}.sql", "r").read())
        cur.execute("commit;")
        cur.execute(open(f"{cwd}/fkidx-{benchmark}.sql", "r").read())
        cur.execute("commit;")
        print("Done.")

        path = f"{cwd}/{benchmark}/queries"
        queries = glob.glob(os.path.join(path, "*.sql"))
        queries.sort()

        # Execution Time Evaluation
        for worker_count in threads:
            print(f"Run {benchmark} with {worker_count} workers...")
            if worker_count == 1:
                # configs from QuerySplit
                if query_split:
                    cur.execute(f"switch to c_r;")
                    cur.execute(f"switch to relationshipcenter;")
                cur.execute(f"set parallel_leader_participation = off;")
                cur.execute(f"set max_parallel_workers = '0';")
                cur.execute(f"set max_parallel_workers_per_gather = '0';")
                cur.execute(f"set shared_buffers = '512MB';")
                cur.execute(f"set temp_buffers = '2047MB';")
                cur.execute(f"set work_mem = '2047MB';")
                cur.execute(f"set effective_cache_size = '4 GB';")
                cur.execute(f"set statement_timeout = '1000s';")
                cur.execute(f"set default_statistics_target = 100;")
                cur.execute("commit;")
            else:
                cur.execute(f"set max_parallel_workers_per_gather = {worker_count}")
                cur.execute("commit;")

            output = "query,duration\n"
            for query_path in queries:
                query_name = query_path.split("/")[-1]
                query = open(query_path).read()
                timings = []
                cur = pg_con.cursor()

                for run in range(nruns):
                    start = time.time()
                    cur.execute(query)
                    end = time.time()
                    duration = end - start
                    results = cur.fetchall()
                    timings.append(duration)
                    cur.execute("commit;")
                    print(f"{query_name} ({run}), query_split({query_split}): {duration:.4f}")

                for timing in timings:
                    output += f"{query_name},{timing:.4f}\n"

            with open(f"{cwd}/experiment-results/4_1_endtoend/{benchmark}/postgres/postgres-{worker_count}"
                      f"-query_split({query_split}).csv", "w") as file:
                file.write(output)

        if check_explain_analyze and not query_split:
            # Intermediate Count Evaluation
            cur.execute(f"set parallel_leader_participation = off;")
            cur.execute(f"set max_parallel_workers = '0';")
            cur.execute(f"set max_parallel_workers_per_gather = '0';")
            cur.execute(f"set shared_buffers = '512MB';")
            cur.execute(f"set temp_buffers = '2047MB';")
            cur.execute(f"set work_mem = '2047MB';")
            cur.execute(f"set effective_cache_size = '4 GB';")
            cur.execute(f"set statement_timeout = '1000s';")
            cur.execute(f"set default_statistics_target = 100;")
            cur.execute("commit;")

            output = "query,join_intermediates\n"
            for query_path in queries:
                query_name = query_path.split("/")[-1]
                query = open(query_path).read()
                cur.execute(f"EXPLAIN (FORMAT JSON, ANALYZE, COSTS 0) {query}")
                query_plan = cur.fetchone()[0][0]
                cur.execute("commit;")
                output += f"{query_name},{extract_join_intermediates(query_plan['Plan'])}\n"

            sp.call(["mkdir", "-p", f"{cwd}/experiment-results/3_5_intermediates/{benchmark}/postgres"])
            with open(f"{cwd}/experiment-results/3_5_intermediates/{benchmark}/postgres/postgres.csv", "w") as file:
                file.write(output)

    cur.close()
    pg_con.close()


if __name__ == "__main__":
    run_postgres()
    run_postgres(True)
