import shutil
import duckdb
import sys
from pathlib import Path
from utils import measure_times, measure_ost, duckdb_discard_result, hyper_discard_result, lineitems_table_def
from tableauhyperapi import HyperProcess, Telemetry, Connection

path_to_tpch = "./tpch_1gb.hyper"

default_params = {
    "experimental_holistic_aggregates": "1",
    "experimental_copy_to": "1",
    "log_scheduler": "1",
    # We force Hyper to be always parallel. Otherwise we would see a cliff at 100,000 tuples
    # because Hyper only enables schedules work on more than one thread for queries
    # with more than 100,000 tuples.
    "parallel": "f",
}

algorithm_params = {
    "naive": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "0", "incremental_windowed_countd": "0"},
    "incremental": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "1", "incremental_windowed_countd": "1"},
    "tree": {"holistic_aggregates_baseline": "0"},
}

functions = {
    "countd": {"call": "COUNT(DISTINCT l_partkey)", "algs": ["naive", "incremental", "tree"]},
    "percentile99": {"call": "HOLISTIC_PERCENTILE_DISC(0.99 ORDER BY l_extendedprice)", "call_duckdb": "quantile_disc(l_extendedprice, 0.99)", "ost_percentile": 99, "algs": ["naive", "incremental", "tree"]},
    "percentile50": {"call": "HOLISTIC_PERCENTILE_DISC(0.5 ORDER BY l_extendedprice)", "call_duckdb": "quantile_disc(l_extendedprice, 0.5)", "ost_percentile": 50, "algs": ["naive", "incremental", "tree"]},
    "rank": {"call": "HOLISTIC_RANK(ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
    "first_value": {"call": "HOLISTIC_FIRST_VALUE(l_extendedprice ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
    "lead": {"call": "HOLISTIC_LEAD(l_extendedprice ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
}

input_sizes = [1] + [i * 20 * 1000 for i in range(1,101)]

window_size_fraction = 0.05 # 5 percent
def get_window_size(input_size):
    window_size = int(window_size_fraction * input_size)
    if window_size < 1:
        # Hyper's optimizer constant folds aggregates if the window frame contains only the current row
        window_size = 1
    return window_size


# Execute all the benchmark queries
print("function,implementation,input_size,window_size,time")
for func_name in functions.keys():
    # Ordered Statistics Tree
    if "ost_percentile" in functions[func_name]:
        percentile = functions[func_name]["ost_percentile"]
        for input_size in input_sizes:
            file_name = f"lineitem_sample_{input_size}.csv"
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=default_params) as hyper:
                with Connection(endpoint=hyper.endpoint, database=path_to_tpch) as connection:
                    connection.execute_command(f"COPY (SELECT * FROM lineitem ORDER BY random() LIMIT {input_size}) TO '{file_name}'")
            window_size = get_window_size(input_size)
            print(f"{func_name},OST,{input_size},{window_size},", end="")
            exec_time = measure_ost(file_name, percentile, window_size)
            print(exec_time)
            sys.stdout.flush()
            throughput = input_size / exec_time
            if throughput < 50*1000 and input_size > 1:
                break

    # DuckDB
    func_snippet_duckdb = functions[func_name].get("call_duckdb")
    if func_snippet_duckdb:
        cursor = duckdb.connect()
        cursor.execute(lineitems_table_def)
        cursor.execute("COPY lineitem FROM '{script_dir}/lineitems_1gb.csv' WITH(DELIMITER '\t');".format(script_dir = Path(__file__).resolve().parent))
        for alg_name in ["separate", "window"]:
            for input_size in input_sizes:
                window_size = get_window_size(input_size)
                print(f"{func_name},DuckDB {alg_name},{input_size},{window_size},", end="")
                cursor.execute(f"DROP TABLE IF EXISTS lineitem_sample");
                cursor.execute(f"CREATE TABLE lineitem_sample AS SELECT * FROM lineitem ORDER BY random() LIMIT {input_size}");
                query_duckdb = f"SELECT {func_snippet_duckdb} OVER (ORDER BY l_shipdate ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) FROM lineitem_sample"
                cursor.execute(f"PRAGMA debug_window_mode='{alg_name}'")
                try:
                    exec_time = measure_times(lambda: cursor.execute(duckdb_discard_result(query_duckdb)).fetchall(), timeout=10)
                except RuntimeError as e:
                    print(e)
                    break
                throughput = input_size / exec_time
                print(throughput)
                if throughput < 50*1000 and input_size > 1:
                    break
        cursor.close()

    # Hyper
    func_snippet = functions[func_name]["call"]
    for alg_name in functions[func_name]["algs"]:
        params = {**default_params, **algorithm_params[alg_name]}
        for input_size in input_sizes:
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=params) as hyper:
                with Connection(endpoint=hyper.endpoint, database=path_to_tpch) as connection:
                    window_size = get_window_size(input_size)
                    print(f"{func_name},Hyper {alg_name},{input_size},{window_size},", end="")
                    connection.execute_command(f"CREATE TEMPORARY TABLE lineitem_sample AS (SELECT * FROM lineitem ORDER BY random() LIMIT {input_size})")
                    query_hyper = f"SELECT {func_snippet} OVER (ORDER BY l_shipdate ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) FROM lineitem_sample"
                    exec_time = measure_times(lambda: connection.execute_list_query(hyper_discard_result(query_hyper)), timeout=10)

            throughput = input_size / exec_time
            if throughput < 50*1000 and input_size > 1:
                break
