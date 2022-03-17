# Supplement for the paper "Efficient Evaluation of Arbitrarily-Framed Holistic SQL Aggregates and Window Functions"

This repository accompanies the paper "Efficient Evaluation of Arbitrarily-Framed Holistic SQL Aggregates and Window Functions"
published at SIGMOD 2022.

It contains:
 
* a stand-alone implementation of mergesort trees and the accompanying algorithms
* benchmark scripts used to benchmark the various systems
* the results in CSV format, as measured on our system

While the paper is published under my Salesforce/Tableau affiliation, neither Salesforce nor Tableau provide any waranty or support for any contents of this repository.
For questions, please reach out to me under avogelsgesang@salesforce.com or file a Github issue.

## Standalone implementation

The subfolder `standalone-implementation` contains a standalone implementation of mergesort trees and Wesley and Xu's incremental algorithms, also including correctness test cases.
The core mergesort tree data structure can be found in the file `mergesorttree.hpp`.
`aggregatedistinct.hpp`, `percentile.hpp` and `rank.hpp` use the mergesort tree to compute holistic aggregates.

We used `scalability-bench.cpp` to evaluate the parameters `f` (fanout) and `k` (sampling rate for inter-level pointers).
Most other experiments from the paper are based on an implementation integrated with Hyper, and can be measured through the freely available [HyperAPI](https://help.tableau.com/current/api/hyper_api/en-us/index.html).
Hyper's implementation is based on the data structure in `mergesorttree.hpp` but adds additional functionality.
E.g., in contrast to the standalone implementation, the implementation in Hyper is multi-threaded (as described in the paper).

## Benchmark scripts

The evaluation scripts are mostly in the subfolder `evaluation`.
We evaluated systems end-to-end, and you can reproduce the measurements for Hyper (our system), DuckDB and Postgres running the provided scripts.
The scripts reference `lineitems_20k.csv` and `lineitems_1gb.csv`.
Those files are not checked in due to file size, I trust you already have the tpc-h data at scale factor 1 anyway.
The subfolder `evaluation/ost-percentile` contains a standalone implementation of a percentile computations using an ordered statistics tree based on Intels' oneTBB and on Simon Tatham's [counted btrees](https://www.chiark.greenend.org.uk/~sgtatham/algorithms/cbtree.html). 
In addition to the `evaluation` folder, `scalability_bench.cpp` from the `standalone-implementation` folder was used for the measurements on fanout, sampling rate and memory consumption presented in the paper.

Make sure to set the CPU governor to `performance` (`sudo cpupower frequency-set -g performance`).


## Benchmark results

Our measurement results are checked-in as CSV files inside the `results` subfolder.
The Tableau workbook `results/evaluation.twb` was used to visualize the measurement results.
Some of the CSV files contain additional data/dimensions which aren't shown in the paper due to space constraints.

## License

All content in this repository, with the exception of `evaluation/ost-percentile/tree234.h`, `evaluation/ost-percentile/tree234.c` and `standalone-implementation/thirdparty/catch.hpp`, is licensed under the MIT license (full license text below).
For the files `evaluation/ost-percentile/tree234.h`, `evaluation/ost-percentile/tree234.c` and `standalone-implementation/thirdparty/catch.hpp`, the orginal licenses put in place by their respective original authors apply.

### MIT License Text

Copyright 2022 salesforce.com, inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
