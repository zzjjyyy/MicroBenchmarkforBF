#pragma once

#include <string>
#include <vector>

class ColumnName {
public:
  /**
   * Converts column name to its canonical form - i.e. lower case
   *
   * @param columnName
   * @return
   */
  static std::string canonicalize(const std::string &columnName);

  static std::vector<std::string> canonicalize(const std::vector<std::string> &columnNames);
};