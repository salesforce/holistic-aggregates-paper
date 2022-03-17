from pathlib import Path
from utils import measure_times, duckdb_discard_result, hyper_discard_result, lineitems_table_def
import duckdb
import psycopg
import tableauhyperapi as hapi

script_dir = Path(__file__).resolve().parent

scenarios = [
    {"name": "20k", "input": "lineitems_20k.csv", "window_size": 999},
    {"name": "1gb_small", "input": "lineitems_1gb.csv", "window_size": 999},
    {"name": "1gb", "input": "lineitems_1gb.csv", "window_size": 9999},
]

setup_sql = [
    lineitems_table_def,
    "COPY lineitem FROM '{script_dir}/{input}' WITH(DELIMITER '\t');"
]
# lineitems_20k.csv was generated as `
# > COPY(SELECT * FROM lineitem ORDER BY random() LIMIT 20000) TO 'lineitems_20k.csv'

query_correlated = """
WITH lineitem_numbered AS (
   SELECT *, ROW_NUMBER() OVER (ORDER BY l_shipdate) AS rn FROM lineitem)
SELECT (
   SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY l_extendedprice)
   FROM lineitem_numbered l2
   WHERE l2.rn BETWEEN l1.rn - {window_size} AND l1.rn
)
FROM lineitem_numbered l1
"""

query_join = """
WITH lineitem_numbered AS (
   SELECT *, ROW_NUMBER() OVER (ORDER BY l_shipdate) AS rn FROM lineitem)
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY l2.l_extendedprice)
FROM lineitem_numbered l1
   JOIN lineitem_numbered l2 ON l2.rn BETWEEN l1.rn - {window_size} AND l1.rn
GROUP BY l1.rn
"""

query_holistic = """
SELECT holistic_percentile_disc(
    0.5 ORDER BY l_extendedprice
  ) OVER (ORDER BY l_shipdate
    ROWS BETWEEN {window_size} preceding AND current row)
FROM lineitem
"""

query_holistic_duckdb = """
SELECT quantile_disc(l_extendedprice, 0.5) OVER (
    ORDER BY l_shipdate
    ROWS BETWEEN {window_size} preceding AND current row)
FROM lineitem
"""


for scenario in scenarios:
    scenario_name = scenario["name"]
    sql_args = scenario
    sql_args.update({"script_dir": script_dir})
    execute_traditional_sql = sql_args["window_size"] < 5000 and sql_args["name"] == "20k"

    def print_header(impl_name):
        print(f"{scenario_name},{impl_name},", end="")

    #############################################
    # Postgres
    #############################################

    if execute_traditional_sql:
        with psycopg.connect() as conn:
            with conn.cursor() as cur:
                for s in setup_sql:
                    cur.execute(s.format(**sql_args))
        
                # We are not really interested in the query result, but only in the evaluation time.
                # We tried a `COPY TO`, a `CREATE TEMP TABLE AS` and a straightforward execution of the query.
                # There were no significant performance differences
                print_header("Postgres correlated")
                measure_times(lambda: cur.execute(query_correlated.format(**sql_args)))
                print_header("Postgres join")
                measure_times(lambda: cur.execute(query_join.format(**sql_args)))
    
    
    #############################################
    # DuckDB
    #############################################
    
    cursor = duckdb.connect()
    for s in setup_sql:
        cursor.execute(s.format(**sql_args))

    if execute_traditional_sql:
        print_header("DuckDB correlated")
        measure_times(lambda: cursor.execute(duckdb_discard_result(query_correlated.format(**sql_args))).fetchall())
        print_header("DuckDB join")
        measure_times(lambda: cursor.execute(duckdb_discard_result(query_join.format(**sql_args))).fetchall())

    print_header("DuckDB window")
    cursor.execute(f"PRAGMA debug_window_mode='window'")
    measure_times(lambda: cursor.execute(duckdb_discard_result(query_holistic_duckdb.format(**sql_args))).fetchall())

    print_header("DuckDB segment tree")
    cursor.execute(f"PRAGMA debug_window_mode='combine'")
    measure_times(lambda: cursor.execute(duckdb_discard_result(query_holistic_duckdb.format(**sql_args))).fetchall())

    if sql_args["window_size"] < 2000:
        # For large window sizes, DuckDB reports an error
        print_header("DuckDB naive")
        cursor.execute(f"PRAGMA debug_window_mode='separate'")
        measure_times(lambda: cursor.execute(duckdb_discard_result(query_holistic_duckdb.format(**sql_args))).fetchall())

    cursor.close()

    #############################################
    # Hyper
    #############################################

    hyper_params = {
        "experimental_holistic_aggregates": "1",
        "experimental_copy_to": "1",
    }
    if execute_traditional_sql:
        with hapi.HyperProcess(telemetry=hapi.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=hyper_params) as hyper, \
             hapi.Connection(endpoint=hyper.endpoint) as connection:
            for s in setup_sql:
                connection.execute_command(s.format(**sql_args))
            print_header("Hyper correlated")
            measure_times(lambda: connection.execute_list_query(hyper_discard_result(query_correlated.format(**sql_args))))
            print_header("Hyper join")
            measure_times(lambda: connection.execute_list_query(hyper_discard_result(query_join.format(**sql_args))))
    
    algorithms = {
        "naive": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "0", "incremental_windowed_countd": "0"},
        "incremental": {"holistic_aggregates_baseline": "1", "incremental_nthvalue": "1", "incremental_windowed_countd": "1"},
        "tree": {"holistic_aggregates_baseline": "0"},
    }
    for alg_name, alg_params in algorithms.items():
        params = {**hyper_params, **alg_params}
        with hapi.HyperProcess(telemetry=hapi.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=params) as hyper, \
             hapi.Connection(endpoint=hyper.endpoint) as connection:
            for s in setup_sql:
                connection.execute_command(s.format(**sql_args))
            print_header(f"Hyper {alg_name}")
            measure_times(lambda: connection.execute_list_query(hyper_discard_result(query_holistic.format(**sql_args))))
