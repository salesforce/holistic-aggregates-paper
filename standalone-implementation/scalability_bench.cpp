#include "data.hpp"
#include "framebounds.hpp"
#include "percentile.hpp"
#include "rank.hpp"
#include "mergesorttree.hpp"
#include <array>
#include <chrono>
#include <cmath>
#include <new>
#include <vector>

using namespace std;
using namespace std::chrono;

template <class T> void doNotOptimizeAway(T &&value) {
  asm volatile("" : "+m,r"(value) : : "memory");
}

template <typename A> static double timeIt(A algorithm) {
  auto start = high_resolution_clock::now();
  doNotOptimizeAway(algorithm());
  auto end = high_resolution_clock::now();
  return duration_cast<duration<double>>(end - start).count();
}

template <typename A, typename T>
static void repeatedTiming(const string &prefix, T inputs, A algorithm) {
  constexpr int64_t repetitions = 5;
  try {
    for (auto &input : inputs) {
      double sum = 0;
      for (auto i = 0; i < repetitions; ++i) {
        cout << prefix << '\t' << input.size() << '\t' << i << '\t';
        double time =
            timeIt([&input, algorithm]() { return algorithm(input); });
        cout << time << endl;
        sum += time;
        if (sum > 5)
          break;
      }
      if (sum > 5)
        break;
    }
  } catch (std::bad_alloc &) {
    cout << "out of memory" << endl;
  }
}

struct benchBuild {
  template <int64_t fanout, int64_t cascading, typename T>
  static void execute(const T &inputs) {
    string prefix = "build\t" + to_string(fanout) + '\t' + to_string(cascading);
    repeatedTiming(prefix, inputs, [](const auto &in) {
      auto copy = in;
      return MergeSortTree<fanout, cascading, typename std::decay_t<decltype(in)>::value_type>(move(copy));
    });
  }
};

struct benchRankUnbounded {
  template <int64_t fanout, int64_t cascading, typename T>
  static void execute(const T &inputs) {
    string prefix =
        "rank_unbounded\t" + to_string(fanout) + '\t' + to_string(cascading);
    repeatedTiming(prefix, inputs, [](const auto &in) {
      return mergesortRank<fanout, cascading>(
          in, framebounds::unboundedPreceding, framebounds::untilCurrentRow);
    });
  }
};

struct benchRank2000 {
  template <int64_t fanout, int64_t cascading, typename T>
  static void execute(const T &inputs) {
    string prefix =
        "rank_2000\t" + to_string(fanout) + '\t' + to_string(cascading);
    repeatedTiming(prefix, inputs, [](const auto &in) {
      return mergesortRank<fanout, cascading>(in, framebounds::nPreceding<2000>,
                                              framebounds::untilCurrentRow);
    });
  }
};

struct benchMedianUnbounded {
  template <int64_t fanout, int64_t cascading, typename T>
  static void execute(const T &inputs) {
    string prefix =
        "median_unbounded\t" + to_string(fanout) + '\t' + to_string(cascading);
    repeatedTiming(prefix, inputs, [](const auto &in) {
      return mergesortPercentile<fanout, cascading>(
          in, framebounds::unboundedPreceding, framebounds::untilCurrentRow,
          0.5);
    });
  }
};

template <typename B, int64_t fanout, int64_t cascading, typename T>
static void testCascadings(const T &inputs) {
  if constexpr (cascading > 0) {
    testCascadings<B, fanout, cascading / 2>(inputs);
    B::template execute<fanout, cascading>(inputs);
  }
}

template <typename B, int64_t fanout, int64_t cascading, typename T>
static void testFanouts(const T &inputs) {
  if constexpr (fanout >= 2) {
    testFanouts<B, fanout / 2, cascading>(inputs);
    B::template execute<fanout, cascading>(inputs);
  }
}

template <typename B, int64_t fanout, int64_t cascading, typename T>
static void testFanoutsAndCascading(const T &inputs) {
  if constexpr (fanout >= 2) {
    testFanoutsAndCascading<B, fanout / 2, cascading>(inputs);
    testCascadings<B, fanout, cascading>(inputs);
  }
}

int main(int argc, const char **argv) {
  if (argc != 2) {
    cerr << "wrong arguments\n";
    return 1;
  }
  auto experiment = atoi(argv[1]);

  cout << "algorithm\tfanout\tcascading\tinput_size\trun\ttime\n";
  if (experiment == 1) {
    vector<int64_t> sizes{10'000, 100'000, 1'000'000, 10'000'000, 100'000'000};

    vector<vector<int64_t>> inputData;
    for (int64_t size : sizes) {
      inputData.push_back(data::uniformRandomInts(size, size * 5, 1));
    }

    // benchBuild::execute<64, 64>(inputData);
    testFanoutsAndCascading<benchRankUnbounded, 1024, 1024>(inputData);
    testFanoutsAndCascading<benchRank2000, 1024, 1024>(inputData);
  } else {
    vector<int64_t> sizes{1'000'000};

    vector<vector<int64_t>> inputData;
    for (int64_t size : sizes) {
      inputData.push_back(data::uniformRandomInts(size, size * 5, 1));
    }
    if (experiment == 2) {
      testFanoutsAndCascading<benchRankUnbounded, 1024, 1024>(inputData);
      testFanoutsAndCascading<benchRank2000, 1024, 1024>(inputData);
      testFanoutsAndCascading<benchMedianUnbounded, 1024, 1024>(inputData);
    } else if (experiment == 3) {
      // Used to observe memory consumption in `htop`
      benchBuild::execute<16, 4>(inputData);
    } else if (experiment == 4) {
      // Used to observe memory consumption using `time -f '%M'`
      benchBuild::execute<32, 32>(inputData);
    } else if (experiment == 5) {
      // Used to observe memory consumption using `time -f '%M'`
      benchBuild::execute<512, 1>(inputData);
    } else if (experiment == 5) {
      // Used to observe memory consumption using `time -f '%M'`
      benchBuild::execute<1024, 2>(inputData);
    }
  }

  return 0;
}
