#pragma once

#include <arrow/compute/util.h>
#include <arrow/compute/key_hash.h>
#include <arrow/api.h>

class RecordBatchHasher {
public:
  RecordBatchHasher() = default;

  static std::shared_ptr<RecordBatchHasher>
  make(const std::shared_ptr<arrow::Schema> &schema,
       const std::vector<std::string> &columnNames);

  void hash(const std::shared_ptr<arrow::RecordBatch> &recordBatch, uint32_t *hashes);
  void hash(const std::shared_ptr<arrow::RecordBatch> &recordBatch, uint64_t *hashes);

  int64_t getHardwareFlags() const;

private:
  void makeKeyColumns(const std::shared_ptr<arrow::RecordBatch> &recordBatch);

  static constexpr int MiniBatchSize_ = 1 << 10;

  arrow::util::TempVectorStack tempStack_;

  arrow::compute::LightContext encodeCtx_;
  std::vector<arrow::compute::KeyColumnMetadata> colMetadata_;
  std::vector<arrow::compute::KeyColumnArray> cols_;
  std::vector<int> keyIndices_;
};