#pragma once

#include <iostream>
#include <vector>
#include <tuple>

template<typename T1, typename T2>
std::ostream& operator<<(std::ostream& o, const std::pair<T1, T2>& x);

template<typename T1, typename T2>
std::ostream& operator<<(std::ostream& o, const std::tuple<T1, T2>& x);

template<typename T>
std::ostream& operator<<(std::ostream& o, const std::vector<T>& x);

template<typename T, size_t S>
std::ostream& operator<<(std::ostream& o, const std::array<T, S>& x);

template<typename T1, typename T2>
std::ostream& operator<<(std::ostream& o, const std::pair<T1, T2>& x) {
  o << "{" << x.first << ", " << x.second << "}";
  return o;
}

template<typename T1, typename T2>
std::ostream& operator<<(std::ostream& o, const std::tuple<T1, T2>& x) {
  o << "{" << std::get<0>(x) << ", " << std::get<1>(x) << "}";
  return o;
}

template<typename T>
std::ostream& operator<<(std::ostream& o, const std::vector<T>& x) {
  o << "{";
  bool first = true;
  for (auto& e : x) {
    if (!first) o << ", ";
    o << e;
    first = false;
  }
  o << "}";;
  return o;
}

template<typename T, size_t S>
std::ostream& operator<<(std::ostream& o, const std::array<T, S>& x) {
  o << "{";
  bool first = true;
  for (auto& e : x) {
    if (!first) o << ", ";
    o << e;
    first = false;
  }
  o << "}";;
  return o;
}


template<typename T>
void printVector(const std::vector<T>& v) {
  bool first = true;
  for (auto& e : v) {
    if (!first) std::cout << "\t";
    std::cout << e;
    first = false;
  }
  std::cout << std::endl;
}

template<typename T>
void printVectors(const std::vector<std::vector<T>>& v) {
  for (auto& e : v) {
    printVector(e);
  }
}

