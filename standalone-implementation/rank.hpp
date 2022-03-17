#include <vector>
#include <algorithm>
#include <chrono>
#include "mergesorttree.hpp"


template<typename T, typename T1, typename T2>
std::vector<T> naiveRank(const std::vector<T>& inputData, T1 lowerBound, T2 upperBound) {
  std::vector<T> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    auto lower = lowerBound(i, inputData.size());
    auto upper = upperBound(i, inputData.size());
    if (lower > lower) lower = upper;
    auto v = inputData[i];
    int64_t rank = 0;
    for (auto j = lower; j < upper; ++j) {
      rank += inputData[j] < v;
    }
    result.push_back(rank);
  }
  return result;
}


template<int64_t fanout, int64_t cascading, typename T, typename T1, typename T2>
std::vector<T> mergesortRank(const std::vector<T>& inputData, T1 lowerBound, T2 upperBound) {
  auto tree = MergeSortTree<fanout, cascading, T>(std::vector<T>(inputData));

  std::vector<T> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    int64_t lower = lowerBound(i, inputData.size());
    int64_t upper = upperBound(i, inputData.size());
    auto v = inputData[i];
    int64_t rank = tree.aggregateLowerBoundSum(lower, upper, v);
    result.push_back(rank);
  }
  return result;
}
