#pragma once

#include <memory>
#include "TupleSet.hpp"
#include "RecordBatchHasher.hpp"
#include "BloomFilter.hpp"

extern clock_t total_time;

class BloomFilterUseKernel {
public:
  // use arrow blocked bloom filter (reuse hasher made by the caller)
  // static std::shared_ptr<TupleSet>
  static void
  filter(const std::shared_ptr<TupleSet> &tupleSet,
         const std::shared_ptr<BlockedBloomFilter> &bloomFilter,
         const std::shared_ptr<RecordBatchHasher> &hasher);

private:
  // using arrow blocked bloom filter (reuse hasher made by the caller)
  // static std::shared_ptr<arrow::RecordBatch>
  static void
  filterRecordBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                    const std::shared_ptr<BlockedBloomFilter> &bloomFilter,
                    const std::shared_ptr<RecordBatchHasher> &hasher);
};