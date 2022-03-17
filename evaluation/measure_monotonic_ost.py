import subprocess
import re
import sys
from utils import measure_ost
from statistics import median

#window_sizes = [1, 10, 25, 100, 250, 1000, 2500, 10000, 25000, 100000]
window_sizes = list(int(pow(2,i/2.0)) for i in range(0, 41))
# uniquify the rounded sizes. Otherwise, we have some duplicated values due to integer rounding
window_sizes = list(dict.fromkeys(window_sizes))

# Execute all the benchmark queries
for percentile in [50, 99]:
    for window_size in window_sizes:
        print(f"percentile{percentile},OST,{window_size},", end="")
        sys.stdout.flush()
        exec_time = measure_ost("./lineitems_1gb.csv", percentile, window_size)
        print(exec_time)
        sys.stdout.flush()
        if exec_time > 5:
            break
