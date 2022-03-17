#include <array>
#include <utility>
#include <cstdint>

constexpr int64_t ceilPowerOf2(int64_t cnt) {
   return 1 << (64 - __builtin_clzll(cnt-1));
}

constexpr int64_t getTournamentTreeSize(int64_t cnt) {
   return ceilPowerOf2(cnt)-1;
}

// Returns the loser tree and the winner of the initial round
template<typename T, size_t cnt>
std::pair<std::array<T, getTournamentTreeSize(cnt)>, T> makeLoserTree(const std::array<T, cnt>& elements, const T loserValue) {
   constexpr int64_t nodeCount = getTournamentTreeSize(cnt);
   std::array<T, nodeCount> loserTree;
   std::array<T, nodeCount> winnerTree;

   // Build lowest layer of winner/loser tree
   {
      auto* loserTreeBase = loserTree.data() + nodeCount/2;
      auto* winnerTreeBase = winnerTree.data() + nodeCount/2;
      constexpr int64_t baseLevelSize = (nodeCount+1)/2;
      for (int64_t i = 0; i < cnt/2; ++i) {
         auto& el1 = elements[i*2];
         auto& el2 = elements[i*2+1];
         if (el1 < el2) {
            loserTreeBase[i] = el2;
            winnerTreeBase[i] = el1;
         } else {
            loserTreeBase[i] = el1;
            winnerTreeBase[i] = el2;
         }
      }
      if (cnt%2) {
         winnerTreeBase[cnt/2] = elements[cnt-1];
         loserTreeBase[cnt/2] = loserValue;
      }
      for (int64_t i = (cnt+1)/2; i < baseLevelSize; ++i) {
         winnerTreeBase[i] = loserValue;
         loserTreeBase[i] = loserValue;
      }
   }
   // Build the upper layers
   for (int64_t targetIdx = nodeCount/2; targetIdx--;) {
      auto& el1 = winnerTree[targetIdx*2+1];
      auto& el2 = winnerTree[targetIdx*2+2];
      if (el1 < el2) {
         loserTree[targetIdx] = el2;
         winnerTree[targetIdx] = el1;
      } else {
         loserTree[targetIdx] = el1;
         winnerTree[targetIdx] = el2;
      }
   }

   return {loserTree, winnerTree[0]};
}

template<typename T, size_t cnt>
T updateLoserTree(std::array<T, cnt>& losers, int64_t updatedIdx, T newElement) {
   constexpr int64_t nodeCount = getTournamentTreeSize(cnt);
   T newWinner = newElement;
   int64_t idx = updatedIdx + nodeCount;
   do {
      // Go up one level
      idx = (idx-1)/2;
      // Compare against loser of previous round
      if (losers[idx] < newWinner) {
         std::swap(losers[idx], newWinner);
      }
   } while (idx);
   return newWinner;
}
