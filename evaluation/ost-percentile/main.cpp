#include "oneapi/tbb.h"
#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

extern "C" {
#include "tree234.h"
}

static void escape(void *p) { asm volatile("" : : "g"(p) : "memory"); }

static void clobber() { asm volatile("" : : : "memory"); }

template <typename T> std::optional<T> checkedParse(std::string_view s) {
  size_t sz;
  T res;
  auto parseRes = std::from_chars(s.begin(), s.end(), res);
  if (parseRes.ec != std::errc() || parseRes.ptr != s.end()) {
    return {};
  }
  return res;
}

template <typename T> T checkedParse(std::string_view s, const char *error) {
  std::optional<T> v = checkedParse<T>(s);
  if (!v)
    throw error;
  return *v;
}

struct Entry {
  uint64_t shipdate;
  int64_t extendedPrice;
};

std::vector<Entry> loadLineitemsCSV(const std::string &filePath) {
  std::vector<Entry> data;
  data.reserve(20000);
  std::ifstream file(filePath);
  if (!file.is_open()) {
    throw "unable to open input file";
  }
  std::string line;
  while (std::getline(file, line)) {
    // Tokenize the line
    std::string_view shipdateStr, extendedPriceStr;
    {
      int colNr = 0;
      std::string_view remainingLine = line;
      while (true) {
        auto pos = remainingLine.find('\t');
        ++colNr;
        const auto match = remainingLine.substr(0, pos);
        if (colNr == 6) {
          extendedPriceStr = match;
        } else if (colNr == 11) {
          shipdateStr = match;
        }
        if (pos == std::string_view::npos)
          break;
        remainingLine = remainingLine.substr(pos + 1);
      }
      if (colNr != 16) {
        throw "unexpected number of columns in input file";
      }
    }
    // Parse shipdate
    uint64_t shipdate;
    {
      const char *errStr = "invalid date";
      auto firstDash = shipdateStr.find('-');
      if (firstDash == std::string_view::npos)
        throw errStr;
      auto secondDash = shipdateStr.find('-', firstDash + 1);
      if (secondDash == std::string_view::npos)
        throw errStr;
      std::string_view yearStr = shipdateStr.substr(0, firstDash);
      std::string_view monthStr =
          shipdateStr.substr(firstDash + 1, secondDash - firstDash - 1);
      std::string_view dayStr = shipdateStr.substr(secondDash + 1);
      int year = checkedParse<int>(yearStr, errStr);
      int month = checkedParse<int>(monthStr, errStr);
      int day = checkedParse<int>(dayStr, errStr);
      // Bitpack year, month and day into a single integer
      shipdate = (year * 16 + month) * 32 + day;
    }
    // Parse extendedPrice
    auto extendedPriceFloat =
        checkedParse<double>(extendedPriceStr, "invalid price");
    auto extendedPrice = static_cast<int64_t>(extendedPriceFloat * 100);
    // Construct the result
    data.push_back(Entry{shipdate, extendedPrice});
  }
  return data;
}

struct Tree234Deleter {
  void operator()(tree234 *arg) const { freetree234(arg); }
};
using Tree234Ptr = std::unique_ptr<tree234, Tree234Deleter>;

/*
Evaluate
    SELECT holistic_percentile_disc(
        0.5 ORDER BY l_extendedprice
    ) OVER (ORDER BY l_shipdate
        ROWS BETWEEN <windowSize> - 1 preceding AND current row)
    FROM <inputData>
*/
std::unique_ptr<uint64_t[]> evaluateQuery(std::span<const Entry> originalData,
                                          uint64_t windowSize,
                                          double percentile,
                                          uint64_t grainSize) {
  assert(windowSize > 0);
  assert(percentile >= 0);
  assert(percentile <= 0);
  // Copy the data, as we need to sort it but must not modify it in-place
  // Use `unique_ptr` instead of `std::vector` to avoid unnecessary
  // initialization
  size_t dataSize = originalData.size();
  std::unique_ptr<Entry[]> data =
      std::make_unique_for_overwrite<Entry[]>(dataSize);

  oneapi::tbb::parallel_for(
      oneapi::tbb::blocked_range<size_t>(0, dataSize, 20000), [&](auto &task) {
        for (size_t i = task.begin(); i < task.end(); ++i) {
          data[i] = originalData[i];
        }
      });

  // Sort according to window frame, i.e. by `l_shipdate`
  auto windowComparator = [](const Entry &a, const Entry &b) {
    return a.shipdate < b.shipdate;
  };
  oneapi::tbb::parallel_sort(data.get(), data.get() + dataSize,
                             windowComparator);

  // Use `unique_ptr` instead of `std::vector` to avoid unnecessary
  // initialization
  std::unique_ptr<uint64_t[]> medians =
      std::make_unique_for_overwrite<uint64_t[]>(dataSize);

  // Compute the median
  auto medianComparator = [](void *a, void *b) -> int {
    auto &e1 = *reinterpret_cast<const Entry *>(a);
    auto &e2 = *reinterpret_cast<const Entry *>(b);
    if (e1.extendedPrice != e2.extendedPrice) {
      return e1.extendedPrice < e2.extendedPrice ? -1 : 1;
    }
    // Tree234 doesn't support duplicates, so we use a pointer comparison to
    // make all tuples non-equal
    if (&e1 != &e2) {
      return &e1 < &e2 ? -1 : 1;
    }
    return 0;
  };
  bool parallelized = false;
  oneapi::tbb::parallel_for(
      oneapi::tbb::blocked_range<size_t>(0, dataSize, grainSize),
      [&](auto &task) {
        Tree234Ptr tree{newtree234(medianComparator)};
        // Construct start state for frame
        for (uint64_t i = std::max<int64_t>(0, task.begin() - windowSize);
             i < task.begin(); ++i) {
          add234(tree.get(), &data[i]);
        }
        // Process tuples inside frame
        for (uint64_t i = task.begin(); i < task.end(); ++i) {
          add234(tree.get(), &data[i]);
          using namespace std;
          if (i >= windowSize) {
            del234(tree.get(), &data[i - windowSize]);
          }
          auto currentSize = std::min(i, windowSize);
          auto percentilePos = static_cast<size_t>(currentSize * percentile);
          auto *percentileNode =
              reinterpret_cast<Entry *>(index234(tree.get(), percentilePos));
          medians[i] = percentileNode->extendedPrice;
        }
      });

  return medians;
}

int main(int argc, const char **argv) {
  try {
    if (argc != 5)
      throw "not enough arguments";
    std::string filePath = argv[1];
    auto windowSize = checkedParse<uint32_t>(argv[2]);
    if (!windowSize)
      throw "invalid window size";
    auto grainSize = checkedParse<uint32_t>(argv[3]);
    if (!grainSize)
      throw "invalid grain size";
    auto percentileInt = checkedParse<uint32_t>(argv[4]);
    if (!percentileInt)
      throw "invalid percentile";
    if (percentileInt > 100)
      throw "invalid percentile";
    auto percentile = static_cast<double>(*percentileInt) / 100;

    // Load
    std::vector<Entry> data = loadLineitemsCSV(filePath);
    std::cerr << "loaded\n";

    if (!*grainSize)
      grainSize = data.size();

    std::chrono::duration<double, std::milli> overallTime{0};
    for (int i = 0; i < 10; ++i) {
      auto start = std::chrono::steady_clock::now();

      // Evaluate the query
      auto medians = evaluateQuery(data, *windowSize, percentile, *grainSize);
      // Make sure the computations are not optimized out
      escape(medians.get());
      clobber();

      auto end = std::chrono::steady_clock::now();
      auto elapsedMs = std::chrono::duration<double, std::milli>(end - start);
      std::cerr << elapsedMs.count() << "ms\n";
      overallTime += elapsedMs;
      if (overallTime.count() > 10*1000) break;

      // Debug output: print the medians
      if (false) {
        for (uint64_t i = 0; i < data.size(); ++i) {
          std::cerr << medians[i] << std::endl;
        }
      }
    }

  } catch (const char *e) {
    std::cerr << e << '\n';
    return 1;
  }
}
