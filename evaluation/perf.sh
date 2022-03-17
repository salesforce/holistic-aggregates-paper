PERF="perf stat -e task-clock,cycles,instructions,branch-misses,cycle_activity.stalls_l2_miss,cycle_activity.stalls_l3_miss"
#PERF="perf timechart record"

$PERF ./ost-percentile/build/ost_percentile lineitem_sample_90000.csv 4500 20000 50
$PERF ./ost-percentile/build/ost_percentile lineitem_sample_100000.csv 5000 20000 50
#echo ==========================
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_150000.csv 7500 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_160000.csv 8000 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_170000.csv 8500 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_180000.csv 9000 20000 50
#echo ==========================
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_320000.csv 16000 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_330000.csv 16500 20000 50
#echo ==========================
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_630000.csv 31500 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_640000.csv 32000 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_650000.csv 32500 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_660000.csv 33000 20000 50
#echo ==========================
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_1270000.csv 63500 20000 50 
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_1280000.csv 64000 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_1290000.csv 64500 20000 50
#$PERF ./ost-percentile/build/ost_percentile lineitem_sample_1300000.csv 64500 20000 50
