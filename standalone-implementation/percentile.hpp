#include <iostream>
#include <vector>
#include <algorithm>
#include <optional>
#include <cassert>
#include "mergesorttree.hpp"
#include "output.hpp"


template<typename T>
T medianOf3(T a, T b, T c) {
  using namespace std;
  return max(min(a,b), min(max(a,b),c));
}


template<typename IterT, typename Comp = std::less<std::decay_t<decltype(*std::declval<IterT>())>>>
void partitionNth(IterT begin, IterT end, size_t n, Comp comp = {}) {
  assert(n < (end - begin));
  while (begin != end) {
    auto pivot = medianOf3(*begin, *(end-1), *(begin + (end-begin)/2));
    auto middle1 = std::partition(begin, end, [pivot, &comp](auto& a) { return comp(a, pivot); });
    auto middle2 = std::partition(middle1, end, [pivot, &comp](auto& a) { return !comp(pivot, a); });
    auto dist1 = middle1 - begin;
    auto dist2 = middle2 - begin;
    if (dist2 <= n) {
      begin = middle2;
      n -= dist2;
    } else if (n < dist1) {
      end = middle1;
    } else {
      return;
    }
  }
}


template<typename IterT, typename Comp = std::less<std::decay_t<decltype(*std::declval<IterT>())>>>
const auto selectNthValue(const IterT begin, const IterT end, size_t n, Comp comp = {}) {
  partitionNth(begin, end, n, comp);
  return *(begin + n);
}


template<typename T, typename T1, typename T2>
std::vector<T> naivePercentile(const std::vector<T>& inputData, T1 lowerBound, T2 upperBound, float p) {
  std::vector<T> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    auto lower = lowerBound(i, inputData.size());
    auto upper = upperBound(i, inputData.size());
    if (lower >= upper) { 
      result.push_back(~T{0});
    } else {
      auto n = static_cast<int64_t>((upper - lower) * p);
      std::vector<T> elementsCopy(inputData.begin() + lower, inputData.begin() + upper);
      result.push_back(selectNthValue(elementsCopy.begin(), elementsCopy.end(), n));
    }
  }
  return result;
}


template<typename T, typename T1, typename T2>
std::vector<T> incrementalPercentile(const std::vector<T>& inputData, T1 lowerBound, T2 upperBound, float p) {
  std::vector<T> result;
  result.reserve(inputData.size());
  std::vector<const T*> partialSorted;
  int64_t prevLower = 0, prevUpper = 0;
  std::optional<int64_t> prevN;
  for (size_t i = 0; i < inputData.size(); ++i) {
    int64_t lower = lowerBound(i, inputData.size());
    int64_t upper = upperBound(i, inputData.size());
    if (lower >= upper) { 
      result.push_back(~T{0});
    } else {
      // Update
      {
        auto inRange = [&](const T* v) { return (v >= inputData.data() + lower) && (v < inputData.data() + upper); };
        if ((upper == prevUpper + 1) && (lower == prevLower + 1)) {
           // ReplaceIndex
           auto replaceIt = partialSorted.begin();
           while (inRange(*replaceIt)) { ++replaceIt; };
           const T& newElem = inputData[upper - 1];
           if (prevN) {
             const T& prevPivot = *partialSorted[*prevN];
             auto updatedPos = replaceIt - partialSorted.begin();
             if ((newElem < prevPivot) && (updatedPos >= *prevN)) { prevN = {}; }
             if ((newElem > prevPivot) && (updatedPos <= *prevN)) { prevN = {}; }
           }
           *replaceIt = &newElem;
        } else {
           // ReuseIndexes
           auto outIt = partialSorted.begin();
           for (auto it = partialSorted.begin(); it < partialSorted.end(); ++it) {
              *outIt = *it;
              outIt += inRange(*it);
           }
           partialSorted.resize(outIt - partialSorted.begin());
           partialSorted.reserve(upper - lower);
           // Add elements below old window
           for (int64_t j = lower; j < std::min(prevLower, upper); ++j) {
              partialSorted.push_back(inputData.data() + j);
           }
           // Add elements above old window
           for (int64_t j = std::max(lower, prevUpper); j < upper; ++j) {
              partialSorted.push_back(inputData.data() + j);
           }
           // Invalidate cached value
           prevN = {};
        }
      }
      prevLower = lower;
      prevUpper = upper;
      int64_t n = static_cast<int64_t>((upper - lower) * p);
      if (prevN != n) {
         prevN = n;
         partitionNth(partialSorted.begin(), partialSorted.end(), n,  [](const T* a, const T* b) { return *a < *b; });
      }
      result.push_back(*partialSorted[n]);
    }
  }
  return result;
}


template<int64_t fanout, int64_t cascading, typename T, typename T1, typename T2>
std::vector<T> mergesortPercentile(const std::vector<T>& inputData, T1 lowerBound, T2 upperBound, float p) {
  using namespace std;
  // Sort the whole input while keeping track of the indices
  vector<pair<T, size_t>> sortWithIdx;
  sortWithIdx.resize(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    sortWithIdx[i] = make_pair(inputData[i], i);
  }
  sort(sortWithIdx.begin(), sortWithIdx.end());
  vector<T> sorted;
  vector<int64_t> indices;
  sorted.resize(sortWithIdx.size());
  indices.resize(sortWithIdx.size());
  for (size_t i = 0; i < sortWithIdx.size(); ++i) {
    sorted[i] = sortWithIdx[i].first;
    indices[i] = sortWithIdx[i].second;
  }
  auto indexTree = MergeSortTree<fanout, cascading, int64_t>(move(indices));

  vector<T> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    int64_t lower = lowerBound(i, inputData.size());
    int64_t upper = upperBound(i, inputData.size());
    if (lower >= upper) { 
      result.push_back(~T{0});
    } else {
      int64_t n = static_cast<int64_t>((upper - lower) * p);
      result.push_back(sorted[indexTree.selectNth(lower, upper, n)]);
    }
  }
  return result;
}
