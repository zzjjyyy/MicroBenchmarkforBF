#pragma once

#include <arrow/type.h>
#include <memory>
#include "Column.hpp"

class Schema {
public:
  explicit Schema(std::shared_ptr<arrow::Schema> Schema);

  static std::shared_ptr<Schema> make(const std::shared_ptr<arrow::Schema> &schema);
  static std::shared_ptr<Schema> concatenate(const std::vector<std::shared_ptr<Schema>>& schemas);

  [[nodiscard]] const std::shared_ptr<arrow::Schema> &getSchema() const;
  [[nodiscard]] const std::vector<std::shared_ptr<arrow::Field>> &fields() const;

  int getFieldIndexByName(const std::string& name);

  std::vector<std::shared_ptr<Column>> makeColumns();

  std::string showString();

private:
  std::shared_ptr<::arrow::Schema> schema_;
};