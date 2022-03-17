import time
import sys
import subprocess
import re
from statistics import median


lineitems_table_def = """
create temp table lineitem (
   l_orderkey integer not null,
   l_partkey integer not null,
   l_suppkey integer not null,
   l_linenumber integer not null,
   l_quantity decimal(12,2) not null,
   l_extendedprice decimal(12,2) not null,
   l_discount decimal(12,2) not null,
   l_tax decimal(12,2) not null,
   l_returnflag char(1) not null,
   l_linestatus char(1) not null,
   l_shipdate date not null,
   l_commitdate date not null,
   l_receiptdate date not null,
   l_shipinstruct char(25) not null,
   l_shipmode char(10) not null,
   l_comment varchar(44) not null
   -- not relevant for the queries; removed
   --primary key (l_orderkey,l_linenumber)
);"""


def print_debug_progress(*args):
    # print(*args, end="")
    # sys.stdout.flush()
    pass

def measure_times(body, timeout=120):
    times = []
    print_debug_progress("\n(")
    for i in range(11):
        start_time = time.monotonic()
        body()
        end_time = time.monotonic()
        exec_time = end_time - start_time
        print_debug_progress(exec_time, " ")
        times.append(exec_time)
        if sum(times) > timeout:
            break
    print_debug_progress(")\n")
    print(median(times))
    sys.stdout.flush()
    return median(times)


def measure_ost(file_path, percentile, window_size, morsel_size = 20000):
    assert percentile <= 100
    command = ["./ost-percentile/build/ost_percentile", str(file_path), str(window_size), str(morsel_size), str(percentile)]
    process = subprocess.Popen(command,
                         stdout=subprocess.PIPE, 
                         stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
    # print(stdout.decode('utf-8'))
    p = re.compile('(\d+\.\d+)ms')
    times = [float(t)/1000 for t in p.findall(stdout.decode('utf-8'))]
    return median(times)


def duckdb_discard_result(sql):
    # We are not really interested in the query result, but only in the evaluation time.
    # We tried 3 appraoches to discard the results in DuckDB:
    # * `CREATE TEMPORARY TABLE ... AS (<query>); taken from https://duckdb.org/2021/08/27/external-sorting.html
    # * `COPY (<query>) TO 'out.csv`
    # * the plain query
    # For `query_correlated` (20k tuples, 999 window size) the measured times were pretty much identical
    # For `query_holistic_duckdb` on 1GB input and 999 window_size, both COPY and Temp table ran in 15 seconds. The plain query in 22 seconds.
    # We hence went with the `COPY`
    return f"COPY ({sql}) TO 'test.csv'"

def hyper_discard_result(sql):
    # For Hyper, the most efficient way to discard a query result is by using `COPY TO`.
    # `CREATE TABLE AS` is a bad idea because Hyper's `INSERT` operator is not properly parallelized, yet.
    return f"COPY ({sql}) TO 'test.binary' WITH (FORMAT HYPERBINARY)"
