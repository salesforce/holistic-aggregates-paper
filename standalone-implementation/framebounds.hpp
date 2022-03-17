#include <cstdint>
#include <algorithm>

#pragma once

namespace framebounds {
   static int64_t fromCurrentRow(int64_t i, int64_t /*end*/) { return i; }
   static int64_t untilCurrentRow(int64_t i, int64_t /*end*/) { return i+1; }
   static int64_t unboundedPreceding(int64_t /*i*/, int64_t /*end*/) { return 0; }
   static int64_t unboundedFollowing(int64_t /*i*/, int64_t end) { return end; }
   template<int64_t n>
   int64_t nFollowing(int64_t i, int64_t end) { return std::min(i + n, end); }
   template<int64_t n>
   int64_t nPreceding(int64_t i, int64_t end) { return (i < n) ? 0 : (i - n); }
   static int64_t oscillating(int64_t i, int64_t end) { return std::min(i%8*end/7,end); }
}
