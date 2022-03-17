#include <vector>
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>
#include "mergesorttree.hpp"

namespace aggregates {
  template<typename T>
  struct count {
    using StateType = int64_t;
    static StateType init() { return 0; }
    static StateType mergeValue(StateType a, T /*x*/) { return a+1; }
    static StateType removeValue(StateType a, T /*x*/) { return a-1; }
    static StateType merge(StateType a, StateType b) { return a+b; }
  };

  template<typename T>
  struct sum {
    using StateType = T;
    static StateType init() { return 0; }
    static StateType mergeValue(StateType a, T b) { return a+b; }
    static StateType removeValue(StateType a, T b) { return a-b; }
    static StateType merge(StateType a, StateType b) { return a+b; }
  };
}


template<typename Agg, typename T1, typename T2>
std::vector<int64_t> naiveAggregateDistinct(const std::vector<int64_t>& inputData, T1 lowerBound, T2 upperBound) {
  using namespace std;
  vector<int64_t> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    auto lower = lowerBound(i, inputData.size());
    auto upper = upperBound(i, inputData.size());
    auto aggState = Agg::init();
    unordered_set<int64_t> distinctValues;
    for (auto j = lower; j < upper; ++j) {
      if (distinctValues.insert(inputData[j]).second) {
        aggState = Agg::mergeValue(aggState, inputData[j]);
      }
    }
    result.push_back(aggState);
  }
  return result;
}

template<typename T1, typename T2>
std::vector<int64_t> naiveCountDistinct(const std::vector<int64_t>& inputData, T1 lowerBound, T2 upperBound) {
  using namespace std;
  vector<int64_t> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    auto lower = lowerBound(i, inputData.size());
    auto upper = upperBound(i, inputData.size());
    unordered_set<int64_t> distinctValues;
    for (auto j = lower; j < upper; ++j) {
      distinctValues.insert(inputData[j]);
    }
    result.push_back(distinctValues.size());
  }
  return result;
}


template<typename Agg, typename T1, typename T2>
std::vector<int64_t> incrementalAggregateDistinct(const std::vector<int64_t>& inputData, T1 lowerBound, T2 upperBound) {
  using namespace std;
  vector<int64_t> result;
  result.reserve(inputData.size());
  unordered_map<int64_t, int64_t> distinctValues;
  int64_t prevLower = 0;
  int64_t prevUpper = 0;
  auto aggState = Agg::init();
  int64_t nonZero = 0;
  for (size_t i = 0; i < inputData.size(); ++i) {
    // Reset hashtable (using theta = 0.25; from Richard Wesleys paper)
    if (distinctValues.size() > nonZero*4) {
      prevLower = 0;
      prevUpper = 0;
      nonZero = 0;
      aggState = Agg::init();
      distinctValues = {};
    }

    int64_t lower = lowerBound(i, inputData.size());
    int64_t upper = upperBound(i, inputData.size());
    //cout << "lower " << lower << endl;
    //cout << "upper " << upper << endl;
    // Remove
    // below new window
    if (prevLower < lower) {
      for (int64_t j = prevLower; j < min(prevUpper, lower); j++) {
        //cout << " - " << j << endl;
        if (--distinctValues[inputData[j]] == 0) {
          aggState = Agg::removeValue(aggState, inputData[j]);
          --nonZero;
        }
      }
    }
    // above new window
    if (prevUpper > upper) {
      for (int64_t j = max(upper, prevLower); j < prevUpper; j++) {
        //cout << " - " << j << endl;
        if (--distinctValues[inputData[j]] == 0) {
          aggState = Agg::removeValue(aggState, inputData[j]);
          --nonZero;
        }
      }
    }
    // Add
    // below old window
    if (prevLower > lower) {
      for (int64_t j = lower; j < min(prevLower, upper); ++j) {
        //cout << " + " << j << endl;
        if (distinctValues[inputData[j]]++ == 0) {
          aggState = Agg::mergeValue(aggState, inputData[j]);
          ++nonZero;
        }
      }
    }
    // above old window
    if (prevUpper < upper) {
      for (int64_t j = max(lower, prevUpper); j < upper; ++j) {
        //cout << " + " << j << endl;
        if (distinctValues[inputData[j]]++ == 0) {
          aggState = Agg::mergeValue(aggState, inputData[j]);
          ++nonZero;
        }
      }
    }
    // Comute value
    result.push_back(aggState);
    prevLower = lower;
    prevUpper = upper;
  }
  return result;
}


template<typename T1, typename T2>
std::vector<int64_t> incrementalCountDistinct(const std::vector<int64_t>& inputData, T1 lowerBound, T2 upperBound) {
  return incrementalAggregateDistinct<aggregates::count<int64_t>>(inputData, lowerBound, upperBound);
}


std::vector<int64_t> computePrevOffsets(const std::vector<int64_t>& inputData) {
  using namespace std;
  vector<int64_t> result;
  result.reserve(inputData.size());
  unordered_map<int64_t, int64_t> prevIdcs;
  for (size_t i = 0; i < inputData.size(); ++i) {
    auto& prevPos = prevIdcs[inputData[i]];
    result.push_back(prevPos);
    prevPos = i+1;
  }
  return result;
}


template<int64_t fanout, int64_t cascading, typename T1, typename T2>
std::vector<int64_t> mergesortCountDistinct(const std::vector<int64_t>& inputData, T1 lowerBound, T2 upperBound) {
  using namespace std;
  auto mergeSortTree = MergeSortTree<fanout, cascading, int64_t>{computePrevOffsets(inputData)};

  vector<int64_t> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    int64_t lower = lowerBound(i, inputData.size());
    int64_t upper = upperBound(i, inputData.size());
    int64_t countDistinct = mergeSortTree.aggregateLowerBoundSum(lower, upper, lower + 1);
    result.push_back(countDistinct);
  }
  return result;
}


template<int64_t fanout, int64_t cascading, typename Agg, typename T1, typename T2>
std::vector<int64_t> mergesortAggregateDistinct(const std::vector<int64_t>& inputData, T1 lowerBound, T2 upperBound) {
  using namespace std;
  MergeSortTree<fanout, cascading, int64_t> mergeSortTree;
  vector<vector<typename Agg::StateType>> runningAggs;
  {
    auto prevOffsets = computePrevOffsets(inputData);
    vector<tuple<int64_t, int64_t>> zippedPrevOffsets;
    zippedPrevOffsets.reserve(prevOffsets.size());
    for (size_t i = 0; i < prevOffsets.size(); ++i) {
      zippedPrevOffsets.emplace_back(prevOffsets[i], inputData[i]);
    }
    auto zippedMergeSortTree = MergeSortTree<fanout, cascading, tuple<int64_t, int64_t>>(move(zippedPrevOffsets));

    mergeSortTree.tree.reserve(zippedMergeSortTree.tree.size());
    runningAggs.reserve(zippedMergeSortTree.tree.size());
    int64_t levelWidth = 1;
    for (int64_t levelNr = 0; levelNr < zippedMergeSortTree.tree.size(); ++levelNr) {
      auto& zippedLevel = zippedMergeSortTree.tree[levelNr].first;
      vector<int64_t> level;
      vector<typename Agg::StateType> levelAggs;
      level.reserve(zippedLevel.size());
      levelAggs.reserve(zippedLevel.size());
      for (size_t i = 0; i < zippedLevel.size(); i += levelWidth) {
        auto nextLimit = min(zippedLevel.size(), i + levelWidth);
        auto aggState = Agg::init();
        for (int64_t j = i; j < nextLimit; ++j) {
          level.push_back(get<0>(zippedLevel[j]));
          if (get<0>(zippedLevel[j]) < i+1) aggState = Agg::mergeValue(aggState, get<1>(zippedLevel[j]));
          levelAggs.push_back(aggState);
        }
      }
      mergeSortTree.tree.emplace_back(move(level), move(zippedMergeSortTree.tree[levelNr].second));
      runningAggs.push_back(move(levelAggs));
      levelWidth *= fanout;
    }
  }

  vector<int64_t> result;
  result.reserve(inputData.size());
  for (size_t i = 0; i < inputData.size(); ++i) {
    int64_t lower = lowerBound(i, inputData.size());
    int64_t upper = upperBound(i, inputData.size());
    // Compute COUNT DISTINCT using mergesort tree
    auto aggState = Agg::init();
    mergeSortTree.aggregateLowerBound(lower, upper, lower + 1, [&](int64_t level, const auto* begin, const auto* pos) {
      if (pos != begin) {
        //cerr << " " << level << " " << from << " " << to << ": " << (pos - begin) << " -> " <<  runningAggs[level][pos - mergeSortTree[level].data()] << endl;
        aggState = Agg::merge(aggState, runningAggs[level][pos - mergeSortTree.tree[level].first.data() - 1]);
      }
    });
    result.push_back(aggState);
  }
  return result;
}
