#include <unordered_set>
#include "catch.hpp"
#include "aggregatedistinct.hpp"
#include "framebounds.hpp"
#include "data.hpp"

using namespace std;

TEST_CASE("naiveCountDistinct is correct", "[distinct]") {
  auto data = GENERATE_REF(
    make_pair(data::values10, vector<int64_t>{1, 2, 2, 3, 3, 2, 2, 2, 3, 3}),
    make_pair(data::values20, vector<int64_t>{1, 2, 3, 3, 4, 4, 3, 3, 3, 3, 3, 4, 4, 4, 3, 2, 2, 2, 3, 4}));
  auto& input = data.first;
  auto& expected = data.second;
  auto lower = framebounds::nPreceding<3>;
  auto upper = framebounds::untilCurrentRow;
  CHECK(naiveCountDistinct(input, lower, upper) == expected);
}


TEST_CASE("incrementalCountDistinct agrees with naiveCountDistinct", "[distinct]") {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);
  CAPTURE(data);
  auto checkIt = [&](auto bounds, auto lower, auto upper) {
    CAPTURE(bounds);
    CHECK(incrementalCountDistinct(data, lower, upper) == naiveCountDistinct(data, lower, upper));
  };
  checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
  checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
  checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
  checkIt("c,o", framebounds::fromCurrentRow, framebounds::oscillating);
}


TEST_CASE("incrementalAggregateDistinct agrees with naiveAggregateDistinct", "[distinct]") {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);
  CAPTURE(data);
  using sum = aggregates::sum<int64_t>;
  auto checkIt = [&](auto bounds, auto lower, auto upper) {
    CAPTURE(bounds);
    CHECK(incrementalAggregateDistinct<sum>(data, lower, upper) == naiveAggregateDistinct<sum>(data, lower, upper));
  };
  checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
  checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
  checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
  checkIt("c,o", framebounds::fromCurrentRow, framebounds::oscillating);
}


TEMPLATE_TEST_CASE_SIG("mergesortCountDistint agrees with incrementalCountDistinct", "[distinct]",
                       ((unsigned fanout, unsigned cascading), fanout, cascading),
                       (2, 0), (3, 0), (4, 0),
                       (2, 1), (3, 1), (4, 1),
                       (2, 2), (3, 3), (4, 4)) {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);
  CAPTURE(data);
  auto checkIt = [&](auto bounds, auto lower, auto upper) {
    CAPTURE(bounds);
    CHECK(mergesortCountDistinct<fanout, cascading>(data, lower, upper) == incrementalCountDistinct(data, lower, upper));
  };
  checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
  checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
  checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
  checkIt("c,o", framebounds::fromCurrentRow, framebounds::oscillating);
}


TEMPLATE_TEST_CASE_SIG("mergesortAggregateDistint agrees with incrementalAggregateDistinct", "[distinct]",
                       ((unsigned fanout, unsigned cascading), fanout, cascading),
                       (2, 0), (3, 0), (4, 0),
                       (2, 1), (3, 1), (4, 1),
                       (2, 2), (3, 3), (4, 4)) {
  const auto& data = GENERATE_REF(as<vector<int64_t>>{}, data::values10, data::values20);
  CAPTURE(data);
  using sum = aggregates::sum<int64_t>;
  auto checkIt = [&](auto bounds, auto lower, auto upper) {
    CAPTURE(bounds);
    CHECK(mergesortAggregateDistinct<fanout, cascading, sum>(data, lower, upper) == incrementalAggregateDistinct<sum>(data, lower, upper));
  };
  checkIt("u,c", framebounds::unboundedPreceding, framebounds::untilCurrentRow);
  checkIt("c,u", framebounds::fromCurrentRow, framebounds::unboundedFollowing);
  checkIt("p,c", framebounds::nPreceding<3>, framebounds::untilCurrentRow);
  checkIt("c,f", framebounds::fromCurrentRow, framebounds::nFollowing<3>);
  checkIt("c,o", framebounds::fromCurrentRow, framebounds::oscillating);
}
