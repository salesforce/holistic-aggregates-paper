#include "catch.hpp"
#include "percentile.hpp"
#include "framebounds.hpp"
#include "data.hpp"

using namespace std;

TEST_CASE("partitionNth", "[percentile]") {
  auto data = GENERATE(
    // unique, consecutive numbers, even number of elements
    vector<int64_t>{1, 8, 2, 5, 9, 3, 0, 7, 4, 6},
    // non-unique numbers, even number of elements
    vector<int64_t>{2, 2, 1, 4, 5, 1, 6, 1, 8, 3},
    // odd number of elements
    vector<int64_t>{2, 2, 1, 4, 5, 1, 6, 1, 8, 3, 1}
    );
  auto sortedData = data;
  sort(sortedData.begin(), sortedData.end());
  CAPTURE(data, sortedData);

  SECTION("selected individually") {
    auto i = GENERATE(0,1,4,5,7,9);
    CAPTURE(i);
    partitionNth(data.begin(), data.end(), i);
    CAPTURE(data);
    auto e = sortedData[i];
    REQUIRE(data[i] == e);
    CHECK(all_of(data.begin(), data.begin() + i, [e](auto x) { return x <= e; }));
    CHECK(all_of(data.begin() + i + 1, data.end(), [e](auto x) { return x >= e; }));
  }

  SECTION("keeping state between selections") {
    // We keep the state between individual calls, so the array gets
    // gradually reordered. Since we call it on all values, we end up with
    // completely sorted data.
    for (size_t i = 0; i < data.size(); ++i) {
      partitionNth(data.begin(), data.end(), i);
      REQUIRE(data[i] == sortedData[i]);
    }
    REQUIRE(sortedData == data);
  }
}


TEST_CASE("naivePercentile", "[percentile]") {
  SECTION("selects the median value (odd frame size)") {
    vector<int64_t> input   {1, 2, 3, 5, 9, 3, 0, 7, 4, 6};
    vector<int64_t> expected{1, 2, 2, 3, 5, 5, 3, 3, 4, 6};
    auto lower = framebounds::nPreceding<2>;
    auto upper = framebounds::untilCurrentRow;
    CHECK(naivePercentile(input, lower, upper, 0.5) == expected);
  }

  SECTION("selects the median values (even frame size)") {
    vector<int64_t> input   {1, 2, 3, 5, 9, 3, 0, 7, 4, 6};
    vector<int64_t> expected{1, 2, 2, 3, 5, 5, 5, 7, 4, 6};
    auto lower = framebounds::nPreceding<3>;
    auto upper = framebounds::untilCurrentRow;
    CHECK(naivePercentile(input, lower, upper, 0.5) == expected);
  }

  SECTION("selects the 1st quantile") {
    vector<int64_t> input   {1, 2, 3, 5, 9, 3, 0, 7, 4, 6};
    vector<int64_t> expected{1, 1, 1, 2, 2, 2, 1, 2, 2, 2};
    auto lower = framebounds::unboundedPreceding;
    auto upper = framebounds::untilCurrentRow;
    CHECK(naivePercentile(input, lower, upper, 0.25) == expected);
  }
}


TEST_CASE("incrementalPercentile agrees with naivePercentile", "[percentile]") {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);
  auto p = GENERATE(0.25, 0.5, 0.75);
  CAPTURE(data, p);
  auto checkIt = [&](auto bounds, auto lower, auto upper) {
    CAPTURE(bounds);
    CHECK(incrementalPercentile(data, lower, upper, p) == naivePercentile(data, lower, upper, p));
  };
  checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
  checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
  checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
  checkIt("c,o", framebounds::fromCurrentRow, framebounds::oscillating);
}


TEMPLATE_TEST_CASE_SIG("mergesortPercentile", "[percentile]",
                       ((unsigned fanout, unsigned cascading), fanout, cascading),
                       (2, 0), (3, 0), (4, 0),
                       (2, 1), (3, 1), (4, 1),
                       (2, 2), (3, 3), (4, 4)) {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);

  SECTION("agrees with naivePercentile on all quantiles") {
    auto p = GENERATE(0.25, 0.5, 0.75);
    auto checkIt = [&](auto bounds, auto lower, auto upper) {
      CAPTURE(bounds);
      CHECK(mergesortPercentile<fanout,cascading>(data, lower, upper, p) == naivePercentile(data, lower, upper, p));
    };
    checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
    checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
    checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
    checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
    checkIt("c,o", framebounds::fromCurrentRow, framebounds::oscillating);
  }
}
