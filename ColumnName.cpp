#include <algorithm>
#include "ColumnName.hpp"

std::string ColumnName::canonicalize(const std::string &columnName) {
  std::string canonicalColumnName;
  canonicalColumnName.resize(columnName.size());
  std::transform(columnName.begin(), columnName.end(), canonicalColumnName.begin(), tolower);
  return canonicalColumnName;
}

std::vector<std::string> ColumnName::canonicalize(const std::vector<std::string> &columnNames) {
  std::vector<std::string> canonicalColumnNames;
  canonicalColumnNames.reserve(columnNames.size());
  for (const auto &columnName: columnNames) {
    canonicalColumnNames.emplace_back(canonicalize(columnName));
  }
  return canonicalColumnNames;
}