from utils import measure_times, duckdb_discard_result, hyper_discard_result, lineitems_table_def
from tableauhyperapi import HyperProcess, Telemetry, Connection

path_to_tpch = "./tpch_1gb.hyper"

default_params = {
    "experimental_holistic_aggregates": "1",
    "experimental_copy_to": "1",
}

algorithm_params = {
    "naive": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "0", "incremental_windowed_countd": "0"},
    "incremental": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "1", "incremental_windowed_countd": "1"},
    "tree": {"holistic_aggregates_baseline": "0"},
}

functions = {
    "countd": {"call": "COUNT(DISTINCT l_partkey)", "algs": ["incremental", "tree"]},
    "percentile99": {"call": "HOLISTIC_PERCENTILE_DISC(0.99 ORDER BY l_extendedprice)", "algs": ["naive", "incremental", "tree"]},
    "percentile50": {"call": "HOLISTIC_PERCENTILE_DISC(0.5 ORDER BY l_extendedprice)", "algs": ["naive", "incremental", "tree"]},
    #"rank": {"call": "HOLISTIC_RANK(ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
    #"first_value": {"call": "HOLISTIC_FIRST_VALUE(l_extendedprice ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
    #"lead": {"call": "HOLISTIC_LEAD(l_extendedprice ORDER BY l_extendedprice)", "algs": ["naive", "tree"]},
}

window_size = 500
prime1 = 7703
prime2 = 499

# Execute all the benchmark queries
for func_name in functions.keys():
    func_snippet = functions[func_name]["call"]
    for alg_name in functions[func_name]["algs"]:
        params = {**default_params, **algorithm_params[alg_name]}
        for non_monotonicity in (x / 20.0 for x in range(0,21)):
            print(f"{func_name},{alg_name},{non_monotonicity},", end="")
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=params) as hyper:
                with Connection(endpoint=hyper.endpoint, database=path_to_tpch) as connection:
                    query_hyper = f"""
                        SELECT {func_snippet} OVER (
                            ORDER BY l_shipdate
                            ROWS BETWEEN {non_monotonicity}::double precision*mod(l_extendedprice::bigint * {prime1}, {prime2}) PRECEDING
                            AND {window_size} - {non_monotonicity}::double precision*mod(l_extendedprice::bigint * {prime1}, {prime2}) FOLLOWING)
                        FROM lineitem"""
                    exec_time = measure_times(lambda: connection.execute_list_query(hyper_discard_result(query_hyper)))
                    if exec_time > 2.5:
                        break
