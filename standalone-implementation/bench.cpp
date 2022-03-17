#include <iostream>
#include <vector>
#include <type_traits>
#include <array>
#include <chrono>
#include <algorithm>
#include "output.hpp"
#include "mergesorttree.hpp"
#include "framebounds.hpp"
#include "aggregatedistinct.hpp"
#include "rank.hpp"
#include "percentile.hpp"
#include "data.hpp"

using namespace std;
using namespace std::chrono;

template<typename T1, typename T2>
static void testAlgorithm(const char* name, T1& correctResult, T2 algorithm) {
  cout << name << ": ";
  auto start = high_resolution_clock::now();
  auto result = algorithm();
  auto end = high_resolution_clock::now();
  if (result != correctResult) {
    cout << "Incorrect Result!\n";
    cerr << "correct: " << correctResult << endl;
    cerr << "incorrect: " << result << endl;
  } else {
    cout << duration_cast<duration<double>>(end-start).count() << "s\n";
  }
};


#if defined(__i386__)

static __inline__ unsigned long long rdtsc(void)
{
    unsigned long long int x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}

#elif defined(__x86_64__)

static __inline__ unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

#endif

int main(int argc, const char** argv) {

  auto execScenario = [](const auto& data, auto lower, auto upper) {
    //printVector(data);
    if(false) {
      cout << "count\n";
      auto correctResult = incrementalCountDistinct(data, lower, upper);
      testAlgorithm("incremental", correctResult, [&data, lower, upper]() { return incrementalCountDistinct(data, lower, upper); });
      testAlgorithm("merge2,0", correctResult, [&data, lower, upper]() { return mergesortCountDistinct<2,0>(data, lower, upper); });
      testAlgorithm("merge2,1", correctResult, [&data, lower, upper]() { return mergesortCountDistinct<2,1>(data, lower, upper); });
      testAlgorithm("merge2,4", correctResult, [&data, lower, upper]() { return mergesortCountDistinct<2,4>(data, lower, upper); });
      testAlgorithm("merge3,9", correctResult, [&data, lower, upper]() { return mergesortCountDistinct<3,9>(data, lower, upper); });
      testAlgorithm("merge3,1", correctResult, [&data, lower, upper]() { return mergesortCountDistinct<3,1>(data, lower, upper); });
    }

    /*
    {
      cout << "sum\n";
      using sum = aggregates::sum<int64_t>;
      auto correctResult = naiveAggregateDistinct<sum>(data, lower, upper);
      testAlgorithm("naive", correctResult, [&data, lower, upper]() { return naiveAggregateDistinct<sum>(data, lower, upper); });
      testAlgorithm("incremental", correctResult, [&data, lower, upper]() { return incrementalAggregateDistinct<sum>(data, lower, upper); });
      testAlgorithm("merge2", correctResult, [&data, lower, upper]() { return mergesortAggregateDistinct<2, sum>(data, lower, upper); });
      testAlgorithm("merge3", correctResult, [&data, lower, upper]() { return mergesortAggregateDistinct<3, sum>(data, lower, upper); });
      testAlgorithm("merge4", correctResult, [&data, lower, upper]() { return mergesortAggregateDistinct<4, sum>(data, lower, upper); });
      testAlgorithm("merge5", correctResult, [&data, lower, upper]() { return mergesortAggregateDistinct<5, sum>(data, lower, upper); });
    }
    cout << endl;
    if (data.size() < 1024*1024) {
      cout << "rank\n";
      //auto correctResult = naiveRank(data, lower, upper);
      auto correctResult = mergesortRank<2,0>(data, lower, upper);;
      //testAlgorithm("naive", correctResult, [&data, lower, upper]() { return naiveRank(data, lower, upper); });
      testAlgorithm("merge2,0", correctResult, [&data, lower, upper]() { return mergesortRank<2,0>(data, lower, upper); });
      testAlgorithm("merge2,1", correctResult, [&data, lower, upper]() { return mergesortRank<2,1>(data, lower, upper); });
      testAlgorithm("merge2,4", correctResult, [&data, lower, upper]() { return mergesortRank<2,4>(data, lower, upper); });
      testAlgorithm("merge3,9", correctResult, [&data, lower, upper]() { return mergesortRank<3,9>(data, lower, upper); });
      testAlgorithm("merge3,1", correctResult, [&data, lower, upper]() { return mergesortRank<3,1>(data, lower, upper); });
    } else {
      cout << "rank (large)\n";
      auto correctResult = mergesortRank<4096,0>(data, lower, upper);;
      testAlgorithm("merge1k,0", correctResult, [&data, lower, upper]() { return mergesortRank<1024,0>(data, lower, upper); });
      testAlgorithm("merge2k,0", correctResult, [&data, lower, upper]() { return mergesortRank<2*1024,0>(data, lower, upper); });
      testAlgorithm("merge4k,0", correctResult, [&data, lower, upper]() { return mergesortRank<4*1024,0>(data, lower, upper); });
      testAlgorithm("merge8k,0", correctResult, [&data, lower, upper]() { return mergesortRank<8*1024,0>(data, lower, upper); });
      testAlgorithm("merge32k,0", correctResult, [&data, lower, upper]() { return mergesortRank<16*1024,0>(data, lower, upper); });
    }
    cout << endl;
    */

    if (true) {
      cout << "median\n";
      auto correctResult = naivePercentile(data, lower, upper, 0.5);
      testAlgorithm("naive", correctResult, [&data, lower, upper]() { return naivePercentile(data, lower, upper, 0.5); });
      testAlgorithm("incremental", correctResult, [&data, lower, upper]() { return incrementalPercentile(data, lower, upper, 0.5); });
      testAlgorithm("merge2,0", correctResult, [&data, lower, upper]() { return mergesortPercentile<2,0>(data, lower, upper, 0.5); });
      testAlgorithm("merge2,1", correctResult, [&data, lower, upper]() { return mergesortPercentile<2,1>(data, lower, upper, 0.5); });
      testAlgorithm("merge2,4", correctResult, [&data, lower, upper]() { return mergesortPercentile<2,4>(data, lower, upper, 0.5); });
      testAlgorithm("merge3,9", correctResult, [&data, lower, upper]() { return mergesortPercentile<3,9>(data, lower, upper, 0.5); });
      testAlgorithm("merge3,1", correctResult, [&data, lower, upper]() { return mergesortPercentile<3,1>(data, lower, upper, 0.5); });
      cout << endl;
    }
  };

  /*
  for (int i = 0; i < 2000; ++i) {
    cerr << i << endl;
    auto inputData = data::uniformRandomInts(i, 2, 1);
    //auto tree = buildMergeSortTree<2, 4>(vector<int64_t>{inputData});
    //printMergeSortTree<2,4>(tree);
    execScenario(inputData, framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  }
  */


  cout << "values10, unboundedPreceding, currentRow:\n";
  execScenario(data::values10, framebounds::unboundedPreceding, framebounds::untilCurrentRow);

  cout << "values10, currentRow, unboundedFollowing:\n";
  execScenario(data::values10, framebounds::fromCurrentRow, framebounds::unboundedFollowing);

  cout << "values10, currentRow, oscillating:\n";
  execScenario(data::values10, framebounds::unboundedPreceding, framebounds::oscillating);

  cout << "values20, unboundedPreceding, currentRow:\n";
  execScenario(data::values20, framebounds::unboundedPreceding, framebounds::untilCurrentRow);

  cout << "values20, currentRow, unboundedFollowing:\n";
  execScenario(data::values20, framebounds::fromCurrentRow, framebounds::unboundedFollowing);

  cout << "values20, currentRow, oscillating:\n";
  execScenario(data::values20, framebounds::unboundedPreceding, framebounds::oscillating);

  auto uniform10000 = data::uniformRandomInts(10000, 300, 1);
  cout << "uniform 10.000, currentRow, unboundedFollowing:\n";
  execScenario(uniform10000, framebounds::fromCurrentRow, framebounds::unboundedFollowing);

  cout << "uniform 10.000, currentRow, oscillating:\n";
  execScenario(uniform10000, framebounds::unboundedPreceding, framebounds::oscillating);

  auto uniform100000 = data::uniformRandomInts(100000, 1000, 1);
  cout << "uniform 100.000, currentRow, unboundedFollowing:\n";
  execScenario(uniform100000, framebounds::fromCurrentRow, framebounds::unboundedFollowing);

  //cout << "uniform 100.000, currentRow, oscillating:\n";
  //execScenario(uniform100000, framebounds::unboundedPreceding, framebounds::oscillating);

  auto uniform200000 = data::uniformRandomInts(200000, 1000, 1);
  cout << "uniform 200.000, currentRow, unboundedFollowing:\n";
  execScenario(uniform200000, framebounds::fromCurrentRow, framebounds::unboundedFollowing);

  //cout << "uniform 200.000, currentRow, oscillating:\n";
  //execScenario(uniform200000, framebounds::unboundedPreceding, framebounds::oscillating);


  /*
  auto inputData = data::uniformRandomInts(16, 100, 1);
  auto x = mergesortRank<2, 0>(inputData, framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  bool success = false;

  cerr << "naive" << endl;
  for (int64_t i = 0; i < 3; ++i) {
    auto start = high_resolution_clock::now();
    auto y = naiveRank(uniform200000, framebounds::fromCurrentRow, framebounds::unboundedFollowing);
    auto end = high_resolution_clock::now();
    cout << duration_cast<duration<double>>(end-start).count() << "s\n";
    success &= (x == y);
  }

  cerr << "mergesort" << endl;
  for (int64_t i = 0; i < 20; ++i) {
    auto start = high_resolution_clock::now();
    auto y = mergesortRank<2, 1>(uniform200000, framebounds::fromCurrentRow, framebounds::unboundedFollowing);
    auto end = high_resolution_clock::now();
    cout << duration_cast<duration<double>>(end-start).count() << "s\n";
    success &= (x == y);
  }
  cerr << (success ? "passed" : "failed") << endl;
  return success;
  */

  return 0;
}
