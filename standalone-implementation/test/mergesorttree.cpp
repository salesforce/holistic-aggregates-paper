#include <unordered_set>
#include "catch.hpp"
#include "mergesorttree.hpp"

using namespace std;

template<typename IterT>
auto isSorted(IterT begin, IterT end) {
  if (begin == end) return true;
  auto iter = begin;
  ++iter;
  while (iter != end) {
    if (*(iter-1) > *iter) return false;
    ++iter;
  }
  return true;
}

TEMPLATE_TEST_CASE_SIG("MergeSortTree Constructor", "[mergesorttree]",
                       ((unsigned fanout), fanout), (2), (3), (4)) {
  auto data = GENERATE(
    // 8 unique numbers, even number of elements -> will lead to "full" tree
    vector<int64_t>{1, 8, 2, 5, 9, 3, 0, 7},
    // non-unique numbers, even number of elements
    vector<int64_t>{2, 2, 1, 4, 5, 1, 6, 1, 8, 3},
    // number of elements is a prime
    vector<int64_t>{2, 2, 1, 4, 5, 1, 6, 1, 8, 3, 1}
    );
  CAPTURE(data);

  auto smtree = MergeSortTree<fanout, 0, int64_t>(vector<int64_t>{data});
  auto& tree = smtree.tree;
  CHECK(tree.front().first.size() != 0);
  auto levelWidth = 1;
  for (size_t level = 0; level < tree.size(); ++level) {
    REQUIRE(tree[level].first.size() == data.size());
    for (size_t j = 0; j < tree[level].first.size(); j += levelWidth) {
      auto& levelData = tree[level].first;
      CAPTURE(level, levelData, j, levelWidth);
      auto begin = levelData.begin() + j;
      auto end = levelData.begin() + min(j + levelWidth, levelData.size());
      REQUIRE(isSorted(begin, end));
    }
    levelWidth *= fanout;
  }
}
