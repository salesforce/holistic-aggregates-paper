#include <vector>
#include <iomanip>
#include <iostream>
#include <array>
#include <cstdint>
#include <cmath>
#include <cassert>
#include "output.hpp"
#include "losertree.hpp"

#pragma once

constexpr bool fanoutsAlign(int64_t a, int64_t b) {
  if (a == 1 || b == 1) { return true; }
  if (a < b) {
    auto tmp = a;
    a = b;
    b = tmp;
  }
  while (a > b) {
    b *= b;
  }
  return (b % a) == 0;
}


template<int64_t fanout, int64_t cascading, typename ElemT, typename IdxT = int64_t>
struct MergeSortTree {
  // TODO: really needed? Shouldn't all combinations work now?
  static_assert(!cascading || fanoutsAlign(fanout, cascading), "`cascading` and `fanout` must align with each other");

  /// The tree data
  std::vector<std::pair<std::vector<ElemT>, std::unique_ptr<IdxT[]>>> tree;

  public:
  /// Constructor
  MergeSortTree() {}
  /// Constructor
  MergeSortTree(std::vector<ElemT>&& lowestLevel);

  /// Dump
  void dump() const;

  template<typename L>
  void aggregateLowerBound(const int64_t lower, const int64_t upper, const int64_t needle, L aggregate) const;
  int64_t aggregateLowerBoundSum(int64_t lower, int64_t upper, int64_t needle) const;


  size_t selectNth(ElemT lower, ElemT upper, IdxT n) const;
};

// TODO: get rid of this again
template<typename T>
struct loser_traits {};
template<>
struct loser_traits<int64_t> {
   static constexpr auto loser_value = std::numeric_limits<int64_t>::max();
};
template<typename... T>
struct loser_traits<std::tuple<T...>> {
   static constexpr auto loser_value = std::tuple<T...>{loser_traits<T>::loser_value...};
};

template<int64_t fanout, int64_t cascading, typename ElemT, typename IdxT>
MergeSortTree<fanout, cascading, ElemT, IdxT>::MergeSortTree(std::vector<ElemT>&& lowestLevel) {
  using namespace std;
  constexpr bool debug = false;

  int64_t len = lowestLevel.size();
  tree.emplace_back(move(lowestLevel), nullptr);
  int64_t runLength = 1;
  while (runLength < len) {
    // merge previous level to construct new level
    auto newRunLength = runLength * fanout;
    auto& prevLevel = tree.back();
    vector<ElemT> newLevel;
    newLevel.reserve(len);
    int64_t newRunCnt = (len+newRunLength-1)/newRunLength;
    unique_ptr<IdxT[]> cascadingOffsets;
    int64_t cascadingOffsetsInsertPos = 0;
    // TODO: This alloation here overallocates.
    // This is particularly bad for large fanouts and the reason why fanout=512, cascading<=8 runs out of memory in the evaluation.
    if constexpr (cascading > 0) if (newRunLength > cascading) cascadingOffsets = make_unique<IdxT[]>(newRunCnt*(2 + newRunLength/cascading)*fanout);
    if constexpr (debug) cout << "+++++ " << tree.size() << endl;
    // for each newly formed run
    for (int64_t newRunIdx = 0; newRunIdx < newRunCnt; ++newRunIdx) {
      if constexpr (debug) cout << "+++ " << newRunIdx << endl;
      // setup pointers and loser tree
      constexpr pair<ElemT, int64_t> loserElem{loser_traits<ElemT>::loser_value, numeric_limits<int64_t>::max()};
      array<pair<ElemT, int64_t>, fanout> initialElems;
      array<int64_t, fanout> readOffsets;
      array<int64_t, fanout> readLimits;
      for (int64_t i = 0; i < fanout; ++i) {
        int64_t ro = newRunIdx*newRunLength + i*runLength;
        readOffsets[i] = min(ro, len);
        readLimits[i] = min(ro + runLength, len);
        if (readOffsets[i] != readLimits[i]) {
          initialElems[i] = make_pair(prevLevel.first[ro], i);
        } else {
          initialElems[i] = loserElem;
        }
      }
      // merge until all input lists are empty
      auto [loserTree, winner] = makeLoserTree(initialElems, loserElem);
      while (winner != loserElem) {
        if constexpr (debug) cout << winner << "; " << loserTree << endl;
        // insert pointers for fractional cascading
        if constexpr (cascading > 0) {
          if (newRunLength > cascading) {
            if ((newLevel.size() % cascading) == 0) {
              for (int64_t i = 0; i < fanout; ++i) {
                cascadingOffsets[cascadingOffsetsInsertPos++] = (readOffsets[i]);
              }
            }
          }
        }
        // insert smallest element
        newLevel.emplace_back(winner.first);
        // fill new element from corresponding input list
        auto inputRunIdx = winner.second;
        auto& ro = readOffsets[inputRunIdx];
        ro++;
        if (ro < readLimits[inputRunIdx]) {
          winner = updateLoserTree(loserTree, inputRunIdx, make_pair(prevLevel.first[ro], inputRunIdx));
        } else {
          winner = updateLoserTree(loserTree, inputRunIdx, loserElem);
        }
      }
      if constexpr (cascading > 0) if (newRunLength > cascading) {
        // We need two "terminator entries" for this run
        for (int64_t j = 0; j < 2; ++j) {
          for (int64_t i = 0; i < fanout; ++i) {
             cascadingOffsets[cascadingOffsetsInsertPos++] = (readOffsets[i]);
          }
        }
      }
    }
    tree.emplace_back(move(newLevel), move(cascadingOffsets));
    runLength = newRunLength;
  }
}

template<int64_t fanout, int64_t cascading, typename ElemT, typename IdxT>
void MergeSortTree<fanout, cascading, ElemT, IdxT>::dump() const {
  using namespace std;
  auto& out = cerr;
  const char* separator = "    ";
  const char* groupSeparator = " || ";
  int64_t levelWidth = 1;
  int64_t numberWidth = 0;
  for (auto& level : tree) {
    for (auto& e : level.first) {
      if (e) {
         int64_t digits = ceil(log10(fabs(e))) + (e < 0);
         if (digits > numberWidth) numberWidth = digits;
      }
    }
  }
  for (auto& level : tree) {
    // Print the elements themself
    {
      out << 'd';
      for (size_t i = 0; i < level.first.size(); ++i) {
        out << ((i && i % levelWidth == 0) ? groupSeparator : separator);
        out << setw(numberWidth) << level.first[i];
      }
      out << endl;
    }
    // Print the pointers
    if (level.second) {
      int64_t runCnt = (level.first.size()+levelWidth-1)/levelWidth;
      int64_t cascadingIdcsCnt = runCnt*(2 + levelWidth/cascading)*fanout;
      for (int64_t childNr = 0; childNr < fanout; ++childNr) {
        out << " ";
        bool first = true;
        for (int64_t idx = 0; idx < cascadingIdcsCnt; idx += fanout) {
          out << ((idx && ((idx/fanout) % (levelWidth/cascading + 2) == 0)) ? groupSeparator : separator);
          out << setw(numberWidth) << level.second[idx + childNr];
          first = false;
        }
        out << endl;
      }
    }
    levelWidth *= fanout;
  }
}


constexpr int64_t lowestCascadingLevel(int64_t fanout, int64_t cascading) {
  int64_t level = 0;
  int64_t levelWidth = 1;
  while (levelWidth <= cascading) {
    ++level;
    levelWidth *= fanout;
  }
  return level;
}


template<int64_t fanout, int64_t cascading, typename ElemT, typename IdxT>
template<typename L>
void MergeSortTree<fanout, cascading, ElemT, IdxT>::aggregateLowerBound(const int64_t lower, const int64_t upper, const int64_t needle, L aggregate) const {
  constexpr bool debug = false;
  using std::cerr;
  using std::endl;
  assert(upper <= tree[0].first.size());
  assert(lower < upper);
  if constexpr (debug) cerr << "======================================================\n";
  if constexpr (debug) this->dump();
  if constexpr (debug) cerr << "lower " << lower << " upper " << upper << " needle " << needle << endl;
  // Find the entry point into the tree
  int64_t lowerRunIdx = lower;
  int64_t upperRunIdx = upper - 1;
  int64_t levelWidth = 1;
  int64_t level = 0;
  int64_t prevUpperRunIdx;
  int64_t currLower, currUpper;
  if (lowerRunIdx == upperRunIdx) {
    currLower = currUpper = lowerRunIdx;
  } else {
    do {
      prevUpperRunIdx = upperRunIdx;
      lowerRunIdx /= fanout;
      upperRunIdx /= fanout;
      levelWidth *= fanout;
      ++level;
    } while (lowerRunIdx != upperRunIdx);
    currUpper = prevUpperRunIdx * levelWidth / fanout;
    currLower = currUpper;
  }
  if constexpr (debug) cerr << "initial level " << level << endl;
  // Aggregate layers using the cascading indices
  if constexpr (cascading != 0) if (level > lowestCascadingLevel(fanout, cascading))  {
    int64_t lowerCascadingIdx;
    int64_t upperCascadingIdx;
    // Find the initial cascading idcs
    {
      int64_t entryBegin = lowerRunIdx * levelWidth;
      int64_t entryEnd = std::min(entryBegin + levelWidth, static_cast<int64_t>(tree[0].first.size()));
      auto* levelData = tree[level].first.data();
      int64_t entryIdx = std::lower_bound(levelData + entryBegin, levelData + entryEnd, needle) - levelData;
      if constexpr (debug) cerr << "initial entry idx " << entryIdx << endl;
      lowerCascadingIdx = upperCascadingIdx = (entryIdx / cascading + 2 * (entryBegin / levelWidth)) * fanout;
      if constexpr (debug) cerr << "cascading Idx " << upperCascadingIdx << endl;
      // We have to sligthly shift the initial cascading idcs because at the top level we won't be exactly on a boundary
      int64_t correction = (prevUpperRunIdx - upperRunIdx * fanout);
      if constexpr (debug) cerr << "correction " << correction << endl;
      lowerCascadingIdx -= (fanout - correction);
      upperCascadingIdx += correction;
    }
    // Aggregate all layers until we reach a layer without cascading indices
    // For the first layer, we already checked we have cascading indices available, otherwise
    // we wouldn't have even searched the entry points. Hence, we use a `do-while` instead of `while`
    do {
      --level;
      levelWidth /= fanout;
      auto* levelData = tree[level].first.data();
      auto& cascadingIdcs = tree[level+1].second;
      if constexpr (debug) cerr << "level " << level << endl;
      if constexpr (debug) cerr << " currLower " << currLower << " currUpper " << currUpper << endl;
      if constexpr (debug) cerr << " cascading " << lowerCascadingIdx << " " << upperCascadingIdx << endl;
      // Left side of tree
      if constexpr (debug) cerr << " left side\n";
      {
        // Handle all completely contained runs
        lowerCascadingIdx += fanout - 1;
        while (currLower - lower >= levelWidth) {
          if constexpr (debug) cerr << "  currLower " << currLower << "\n";
          if constexpr (debug) cerr << "   cascading idx " << lowerCascadingIdx << " " << cascadingIdcs[lowerCascadingIdx] << " " << cascadingIdcs[lowerCascadingIdx + fanout] << endl;
          // Search based on cascading info from previous level
          const auto* searchBegin = levelData + cascadingIdcs[lowerCascadingIdx];
          const auto* searchEnd = levelData + cascadingIdcs[lowerCascadingIdx + fanout];
          if constexpr (debug) cerr << "   search " << (searchBegin - levelData) << " - " << (searchEnd - levelData) << endl;
          const auto* it = std::lower_bound(searchBegin, searchEnd, needle);
          // Compute runBegin and pass it to our callback
          const auto* runBegin = levelData + currLower - levelWidth;
          aggregate(level, runBegin, it);
          // Update state for next round
          currLower -= levelWidth;
          --lowerCascadingIdx;
        }
        // Handle the partial last run to find the cascading entry point for the next level
        if (currLower != lower) {
          const auto* searchBegin = levelData + cascadingIdcs[lowerCascadingIdx];
          const auto* searchEnd = levelData + cascadingIdcs[lowerCascadingIdx + fanout];
          if constexpr (debug) cerr << "   search cascade " << (searchBegin - levelData) << " - " << (searchEnd - levelData) << endl;
          auto idx = std::lower_bound(searchBegin, searchEnd, needle) - levelData;
          if constexpr (debug) cerr << "   cascade " << (searchBegin - levelData) << " - " << (searchEnd - levelData) << " -> " << idx << endl;
          lowerCascadingIdx = (idx / cascading + 2 * (lower / levelWidth)) * fanout;
        }
      }
      // Right side of tree
      if constexpr (debug) cerr << " right side\n";
      {
        // Handle all completely contained runs
        while (upper - currUpper >= levelWidth) {
          if constexpr (debug) cerr << "  currUpper " << currUpper << "\n";
          // Search based on cascading info from previous level
          const auto* searchBegin = levelData + cascadingIdcs[upperCascadingIdx];
          const auto* searchEnd = levelData + cascadingIdcs[upperCascadingIdx + fanout];
          if constexpr (debug) cerr << "   search " << (searchBegin - levelData) << " - " << (searchEnd - levelData) << endl;
          const auto* it = std::lower_bound(searchBegin, searchEnd, needle);
          // Compute runBegin and pass it to our callback
          const auto* runBegin = levelData + currUpper;
          aggregate(level, runBegin, it);
          // Update state for next round
          currUpper += levelWidth;
          ++upperCascadingIdx;
        }
        // Handle the partial last run to find the cascading entry point for the next level
        if (currUpper != upper) {
          const auto* searchBegin = levelData + cascadingIdcs[upperCascadingIdx];
          const auto* searchEnd = levelData + cascadingIdcs[upperCascadingIdx + fanout];
          auto idx = std::lower_bound(searchBegin, searchEnd, needle) - levelData;
          if constexpr (debug) cerr << "   cascade " << (searchBegin - levelData) << " - " << (searchEnd - levelData) << " -> " << idx << endl;
          upperCascadingIdx = (idx / cascading + 2 * (upper / levelWidth)) * fanout;
        }
      }
    } while (level >= lowestCascadingLevel(fanout, cascading));
  }
  // Handle lower levels which won't have cascading info
  if (level) while (--level) {
    levelWidth /= fanout;
    auto* levelData = tree[level].first.data();
    if constexpr (debug) cerr << "level " << level << endl;
    if constexpr (debug) cerr << " currLower " << currLower << " currUpper " << currUpper << endl;
    // Left side
    while (currLower - lower >= levelWidth) {
      const auto* runEnd = levelData + currLower;
      const auto* runBegin = runEnd - levelWidth;
      const auto* it = std::lower_bound(runBegin, runEnd, needle);
      aggregate(level, runBegin, it);
      currLower -= levelWidth;
    }
    // Right side
    while (upper - currUpper >= levelWidth) {
      const auto* runBegin = levelData + currUpper;
      const auto* runEnd = runBegin + levelWidth;
      const auto* it = std::lower_bound(runBegin, runEnd, needle);
      aggregate(level, runBegin, it);
      currUpper += levelWidth;
    }
  }
  // The last layer
  if constexpr (debug) cerr << "last layer\n";
  {
    auto* levelData = tree[0].first.data();
    // Left side
    auto lowerIt = lower;
    while (lowerIt != currLower) {
      if constexpr (debug) cerr << " lower " << lowerIt << " " << currLower << endl;
      const auto* runBegin = levelData + lowerIt;
      const auto* it = runBegin + (*runBegin < needle);
      aggregate(level, runBegin, it);
      ++lowerIt;
    }
    // Right side
    while (currUpper != upper) {
      if constexpr (debug) cerr << " upper " << currUpper << endl;
      const auto* runBegin = levelData + currUpper;
      const auto* it = runBegin + (*runBegin < needle);
      aggregate(level, runBegin, it);
      ++currUpper;
    }
  }
}


template<int64_t fanout, int64_t cascading, typename ElemT, typename IdxT>
int64_t MergeSortTree<fanout, cascading, ElemT, IdxT>::aggregateLowerBoundSum(int64_t lower, int64_t upper, int64_t needle) const {
  int64_t sum = 0;
  aggregateLowerBound(lower, upper, needle, [&](int64_t level, const auto* begin, const auto* pos) {
    sum += pos - begin;
    //std::cerr << "  ---AGG " << (begin - tree[level].first.data()) << " " << (pos - tree[level].first.data()) << std::endl;
  });
  //std::cerr << "AGG RES " << sum << std::endl;
  return sum;
}

template<int64_t fanout, int64_t cascading, typename ElemT, typename IdxT>
size_t MergeSortTree<fanout, cascading, ElemT, IdxT>::selectNth(const ElemT lower, const ElemT upper, IdxT n) const {
  assert(lower < upper);
  using namespace std;
  constexpr bool debug = false;
  if constexpr (debug) cout << "======================================================\n";
  if constexpr (debug) this->dump();
  if constexpr (debug) cout << "lower " << lower << " upper " << upper << " n " << n << endl;
  // Handle special case of a one-element tree
  if (tree.size() == 1) return 0;
  // The first level contains a single run, so no need to even look at it (except for cascading idcs)
  size_t levelNr = tree.size() - 2;
  size_t levelWidth = 1;
  for (size_t i = 0; i < levelNr; ++i) {
    levelWidth *= fanout;
  }
  // Find our element in a top-down traversal
  size_t traversalIdx = 0;
  // Handle levels with cascading info
  if constexpr (cascading != 0) if (levelNr > lowestCascadingLevel(fanout, cascading))  {
    int64_t lowerCascadingIdx;
    int64_t upperCascadingIdx;
    // Find the initial cascading idcs
    {
      auto& levelData = tree[levelNr+1].first;
      int64_t lowerEntryIdx = std::lower_bound(levelData.begin(), levelData.end(), lower) - levelData.begin();
      lowerCascadingIdx = lowerEntryIdx / cascading * fanout;
      int64_t upperEntryIdx = std::lower_bound(levelData.begin(), levelData.end(), upper) - levelData.begin();
      upperCascadingIdx = upperEntryIdx / cascading * fanout;
    }
    if constexpr (debug) cout << "cascading lvl " << levelNr << " idx " << traversalIdx << " n " << n << endl;
    // Handle all subsequent levels with cascading info attached
    // For the first layer, we already checked we have cascading indices available, otherwise
    // we wouldn't have even searched the entry points. Hence, we use a `do-while` instead of `while`
    do {
      if constexpr (debug) cout << "  lvl " << levelNr << " idx " << traversalIdx << endl;
      auto* levelData = tree[levelNr].first.data();
      auto& cascadingIdcs = tree[levelNr+1].second;
      // Go over all children until we found enough in range
      while (true) {
         auto* lowerSearchBegin = levelData + cascadingIdcs[lowerCascadingIdx];
         auto* lowerSearchEnd = levelData + cascadingIdcs[lowerCascadingIdx + fanout];
         if constexpr (debug) cout << "    lower range " << (lowerSearchBegin - levelData) << "   "  << (lowerSearchEnd - levelData) << endl;
         auto* lowerMatch = std::lower_bound(lowerSearchBegin, lowerSearchEnd, lower);
         if constexpr (debug) cout << "    lower match " << (lowerMatch - levelData) << endl;
         auto* upperSearchBegin = levelData + cascadingIdcs[upperCascadingIdx];
         auto* upperSearchEnd = levelData + cascadingIdcs[upperCascadingIdx + fanout];
         if constexpr (debug) cout << "    upper range " << (upperSearchBegin - levelData) << "   "  << (upperSearchEnd - levelData) << endl;
         auto* upperMatch = std::lower_bound(upperSearchBegin, upperSearchEnd, upper);
         if constexpr (debug) cout << "    upper match " << (upperMatch - levelData) << endl;
         int64_t cntMatches = upperMatch - lowerMatch;
         if constexpr (debug) cout << "    match cnt " << cntMatches << endl;
         if (cntMatches <= n) {
           ++traversalIdx;
           ++lowerCascadingIdx;
           ++upperCascadingIdx;
           n -= cntMatches;
           if constexpr (debug) cout << "    new idx " << traversalIdx << " n " << n << endl;
         } else {
           // Move down to next level in tree
           int64_t upperMatchIdx = upperMatch - levelData;
           upperCascadingIdx = (upperMatchIdx / cascading + 2 * traversalIdx) * fanout;
           int64_t lowerMatchIdx = lowerMatch - levelData;
           lowerCascadingIdx = (lowerMatchIdx / cascading + 2 * traversalIdx) * fanout;
           traversalIdx *= fanout;
           levelWidth /= fanout;
           --levelNr;
           break;
        }
      }
    } while (levelNr >= lowestCascadingLevel(fanout, cascading));
  }
  // Handle lower levels which won't have cascading info
  if constexpr (debug) cout << "non-cascading lvl " << levelNr << " idx " << traversalIdx << " n " << n << endl;
  for (; levelNr; --levelNr) {
    auto& level = tree[levelNr].first;
    auto rangeBegin = level.begin() + traversalIdx * levelWidth;
    auto rangeEnd = rangeBegin + levelWidth;
   if constexpr (debug) cout << "  lvl " << levelNr << "  idx " << traversalIdx <<  endl;
    while (rangeEnd < level.end()) {
      auto matchesFirst = std::lower_bound(rangeBegin, rangeEnd, lower);
      auto matchesLast = std::lower_bound(matchesFirst, rangeEnd, upper);
      auto cntMatches = matchesLast - matchesFirst;
      if constexpr (debug) cout << "    match cnt " << cntMatches << endl;
      if (cntMatches <= n) {
        ++traversalIdx;
        n -= cntMatches;
        if constexpr (debug) cout << "    new idx " << traversalIdx << " n " << n << endl;
      } else {
        // Move down to next level in tree
        break;
      }
      rangeBegin = rangeEnd;
      rangeEnd += levelWidth;
    }
    traversalIdx *= fanout;
    levelWidth /= fanout;
    if constexpr (debug) cout << "    move down " << levelNr << " idx " << traversalIdx << " n " << n << endl;
  }
  // The last level
  if constexpr (debug) cout << "lastLevel idx " << traversalIdx << " n " << n << endl;
  {
    auto* levelData = tree[0].first.data();
    ++n;
    while (true) {
      auto v = levelData[traversalIdx];
      n -= (v >= lower) && (v < upper);
      if (!n) break;
      ++traversalIdx;
    }
  }
  if constexpr (debug) cout << "result " << traversalIdx << endl;
  return traversalIdx;
}
