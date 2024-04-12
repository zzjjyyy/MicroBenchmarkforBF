#pragma once

/*
 * Utilities for working with a single Arrow array
 *
 * @tparam ARROW_ARRAY_TYPE
 * @tparam C_TYPE
 */
template<typename ARROW_ARRAY_TYPE, typename C_TYPE = typename ARROW_ARRAY_TYPE::c_type>
class ArrayHelper {
public:
  static C_TYPE at(const ARROW_ARRAY_TYPE &array, int index) {
    return array.Value(index);
  }
};

template<>
class ArrayHelper<arrow::StringArray, std::string> {
public:

  /**
   * TODO: Maybe this should return a pointer?
   *
   * @param array
   * @param index
   * @return
   */
  static std::string at(const arrow::StringArray &array, int index) {
    return array.GetString(index);
  }
};