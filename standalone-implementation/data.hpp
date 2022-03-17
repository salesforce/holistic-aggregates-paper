#include <vector>
#include <cstdint>
#include <random>

namespace data {
  static const std::vector<int64_t> values10 = {
    1, 2, 1, 3, 1, 1, 1, 2, 3, 1
  };
  static const std::vector<int64_t> values20 = {
    4, 1, 3, 4, 2, 1, 1, 4, 2, 1,
    4, 5, 3, 1, 1, 1, 5, 1, 2, 4
  };

  static std::vector<int64_t> uniformRandomInts(size_t size, int64_t nrDistinct, int64_t seed) {
    std::vector<int64_t> data;
    data.reserve(size);
    std::ranlux48 gen(seed);
    std::uniform_int_distribution<> dis(1, nrDistinct);
    for (size_t i = 0; i < size; ++i) {
      data.push_back(dis(gen));
    }
    return data;
  }
}

