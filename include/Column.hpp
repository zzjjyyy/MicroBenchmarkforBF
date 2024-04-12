#pragma once

#include <arrow/array.h>
#include <string>
#include <memory>
#include "ColumnIterator.hpp"

class Column {
public:
  explicit Column(std::string name, std::shared_ptr<::arrow::ChunkedArray> array);
  Column() = default;
  Column(const Column&) = default;
  Column& operator=(const Column&) = default;

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::Array> &array);

  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::ChunkedArray> &array);

  static std::shared_ptr<Column> make(const std::string &name, const ::arrow::ArrayVector &arrays);

  /**
   * Makes an empty column of the given type
   */
  static std::shared_ptr<Column> make(const std::string &name, const std::shared_ptr<::arrow::DataType> &type);

  static std::vector<std::shared_ptr<::arrow::ChunkedArray>>
  columnVectorToArrowChunkedArrayVector(const std::vector<std::shared_ptr<Column>> &columns);

  [[nodiscard]] const std::string &getName() const;
  void setName(const std::string &Name);
  std::shared_ptr<::arrow::DataType> type();

  long numRows();
  size_t size();

  std::string showString();
  [[nodiscard]] std::string toString() const;

  /**
   * Returns the element in the column at the given row index
   *
   * @param row
   * @return
   */
  std::shared_ptr<Scalar> element(long index);

  ColumnIterator begin();

  ColumnIterator end();

  [[nodiscard]] const std::shared_ptr<::arrow::ChunkedArray> &getArrowArray() const;

private:
  std::string name_;
  std::shared_ptr<arrow::ChunkedArray> array_;
};

using ColumnPtr = std::shared_ptr<Column>;