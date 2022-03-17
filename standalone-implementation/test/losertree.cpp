#include "catch.hpp"
#include "losertree.hpp"

TEST_CASE("makeLoserTree", "[losertree]") {
  SECTION("elements is a power of two") {
    std::array<int64_t, 8> input{2, 1, 3, 5, 4, 3, 7, 6};
    std::array<int64_t, 7> expected{3, 3, 6, 2, 5, 4, 7};
    auto [loserTree, firstWinner] = makeLoserTree<int64_t>(input, 99);
    CHECK(firstWinner == 1);
    CHECK(loserTree == expected);
  }

  SECTION("elements is not a power of two") {
    SECTION("elements is even") {
      std::array<int64_t, 10> input{3, 6, 2, 4, 0, 7, 5, 8, 1, 4};
      std::array<int64_t, 15> expected{1, 2, 99, 3, 5, 99, 99, 6, 4, 7, 8, 4, 99, 99, 99};
      auto [loserTree, firstWinner] = makeLoserTree<int64_t>(input, 99);
      CHECK(firstWinner == 0);
      CHECK(loserTree == expected);
    }

    SECTION("elements is odd") {
      std::array<int64_t, 5> input{2, 3, 5, 1, 6};
      std::array<int64_t, 7> expected{6, 2, 99, 3, 5, 99, 99};
      auto [loserTree, firstWinner] = makeLoserTree<int64_t>(input, 99);
      CHECK(firstWinner == 1);
      CHECK(loserTree == expected);
    }
  }
}

TEST_CASE("updateLoserTree", "[losertree]") {
  std::array<int64_t, 5> input{3, 0, 2, 4, 1};
  auto [loserTree, winner] = makeLoserTree<int64_t>(input, 99);
  CHECK(winner == 0);
  // In tree: 1,2,3,4
  winner = updateLoserTree<int64_t>(loserTree, 1, 1);
  CHECK(winner == 1);
  // In tree: 1,2,3,4
  winner = updateLoserTree<int64_t>(loserTree, 1, 8);
  CHECK(winner == 1);
  // In tree: 2,3,4,8
  winner = updateLoserTree<int64_t>(loserTree, 4, 6);
  CHECK(winner == 2);

  std::array<int64_t, 7> expectedTree{6, 3, 99, 8, 4, 99, 99};
  CHECK(loserTree == expectedTree);
}
