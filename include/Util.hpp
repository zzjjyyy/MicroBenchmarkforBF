#pragma once

#include <arrow/api.h>
#include <memory>

class Util {
public:
    static std::shared_ptr<arrow::Table> calibrateSchema(const std::shared_ptr<arrow::Table> &table,
                                                         const std::shared_ptr<arrow::Schema> &targetSchema);
};