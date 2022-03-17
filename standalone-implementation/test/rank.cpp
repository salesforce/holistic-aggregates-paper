#include "catch.hpp"
#include "rank.hpp"
#include "framebounds.hpp"
#include "data.hpp"

using namespace std;

TEST_CASE("naiveRank", "[rank]") {
  SECTION("simple case 1") {
    vector<int64_t> input   {1, 2, 3, 5, 9, 3, 0, 7, 4, 6};
    vector<int64_t> expected{0, 1, 2, 3, 3, 0, 0, 2, 2, 2};
    auto lower = framebounds::nPreceding<3>;
    auto upper = framebounds::untilCurrentRow;
    CHECK(naiveRank(input, lower, upper) == expected);
  }

  SECTION("selects the median values (even frame size)") {
    vector<int64_t> input   {1, 2, 3, 5, 9, 3, 0, 7, 4, 6};
    vector<int64_t> expected{0, 1, 2, 3, 4, 2, 0, 6, 5, 7};
    auto lower = framebounds::unboundedPreceding;
    auto upper = framebounds::untilCurrentRow;
    CHECK(naiveRank(input, lower, upper) == expected);
  }
}


TEMPLATE_TEST_CASE_SIG("mergesortRank", "[rank]",
                       ((unsigned fanout, unsigned cascading), fanout, cascading),
                       (2, 0), (3, 0), (4, 0),
                       (2, 1), (3, 1), (4, 1),
                       (2, 2), (3, 3), (4, 4)) {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);

  SECTION("agrees with naiveRank") {
    auto checkIt = [&](auto bounds, auto lower, auto upper) {
      CAPTURE(bounds);
      CHECK(mergesortRank<fanout, cascading>(data, lower, upper) == naiveRank(data, lower, upper));
    };
    checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
    checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
    checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
    checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
  }
}
