import shutil
import duckdb
from pathlib import Path
from utils import measure_times, duckdb_discard_result, hyper_discard_result, lineitems_table_def
from tableauhyperapi import HyperProcess, Telemetry, Connection

path_to_tpch = "./tpch_1gb.hyper"
log_dir = Path("./logs")
if log_dir.exists():
    shutil.rmtree(log_dir)

default_params = {
    "experimental_holistic_aggregates": "1",
    "experimental_copy_to": "1",
    "log_scheduler": "1",
}

algorithm_params = {
    "naive": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "0", "incremental_windowed_countd": "0"},
    "incremental": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "1", "incremental_windowed_countd": "1"},
    "tree": {"holistic_aggregates_baseline": "0"},
}

functions = {
    "countd": {"call": "COUNT(DISTINCT l_partkey)", "algs": ["naive", "incremental", "tree"]},
    "percentile99": {"call": "HOLISTIC_PERCENTILE_DISC(0.99 ORDER BY l_extendedprice)", "call_duckdb": "quantile_disc(l_extendedprice, 0.99)", "algs": ["naive", "incremental", "tree"]},
    "percentile50": {"call": "HOLISTIC_PERCENTILE_DISC(0.5 ORDER BY l_extendedprice)", "call_duckdb": "quantile_disc(l_extendedprice, 0.5)", "algs": ["naive", "incremental", "tree"]},
    "rank": {"call": "HOLISTIC_RANK(ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
    "first_value": {"call": "HOLISTIC_FIRST_VALUE(l_extendedprice ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
    "lead": {"call": "HOLISTIC_LEAD(l_extendedprice ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
}

#window_sizes = [1, 10, 25, 100, 250, 1000, 2500, 10000, 25000, 100000]
window_sizes = list(int(pow(2,i/2.0)) for i in range(0, 41))
# uniquify the rounded sizes. Otherwise, we have some duplicated values due to integer rounding
window_sizes = list(dict.fromkeys(window_sizes))

# Execute all the benchmark queries
for func_name in functions.keys():
    # DuckDB
    func_snippet_duckdb = functions[func_name].get("call_duckdb")
    if func_snippet_duckdb:
        cursor = duckdb.connect()
        cursor.execute(lineitems_table_def)
        cursor.execute("COPY lineitem FROM '{script_dir}/lineitems_1gb.csv' WITH(DELIMITER '\t');".format(script_dir = Path(__file__).resolve().parent))
        for alg_name in ["separate", "window"]:
            for window_size in window_sizes:
                query_duckdb = f"SELECT {func_snippet_duckdb} OVER (ORDER BY l_shipdate ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) FROM lineitem"
                print(f"{func_name},DuckDB {alg_name},{window_size},", end="")
                cursor.execute(f"PRAGMA debug_window_mode='{alg_name}'")
                exec_time = measure_times(lambda: cursor.execute(duckdb_discard_result(query_duckdb)).fetchall(), timeout=10)
                if exec_time > 5:
                    break
        cursor.close()


    # Hyper
    func_snippet = functions[func_name]["call"]
    for alg_name in functions[func_name]["algs"]:
        params = {**default_params, **algorithm_params[alg_name]}
        for window_size in window_sizes:
            log_path = log_dir / f"{func_name}_{alg_name}_{window_size}"
            log_path.mkdir(parents=True)
            params["log_dir"] = str(log_path)
            print(f"{func_name},Hyper {alg_name},{window_size},", end="")
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=params) as hyper:
                with Connection(endpoint=hyper.endpoint, database=path_to_tpch) as connection:
                    query_hyper = f"SELECT {func_snippet} OVER (ORDER BY l_shipdate ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW) FROM lineitem"
                    exec_time = measure_times(lambda: connection.execute_list_query(hyper_discard_result(query_hyper)), timeout=10)

            if exec_time > 5:
                break
