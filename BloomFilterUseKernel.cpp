#include "BloomFilterUseKernel.hpp"

// std::shared_ptr<TupleSet>
void
BloomFilterUseKernel::filter(const std::shared_ptr<TupleSet> &tupleSet,
                             const std::shared_ptr<BlockedBloomFilter> &bloomFilter,
                             const std::shared_ptr<RecordBatchHasher> &hasher) {
  // filter batches
  arrow::RecordBatchVector filteredBatches;
  arrow::TableBatchReader reader{*tupleSet->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    assert(false);
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    // auto expFilteredBatch = filterRecordBatch(recordBatch, bloomFilter, hasher);
    filterRecordBatch(recordBatch, bloomFilter, hasher);
    // if (expFilteredBatch == nullptr) {
    //   assert(false);
    // }
    // filteredBatches.emplace_back(expFilteredBatch);

    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      assert(false);
    }
    recordBatch = *expRecordBatch;
  }

  // make output tupleSet
  // auto expTable = arrow::Table::FromRecordBatches(filteredBatches);
  // if (!expTable.ok()) {
  //   assert(false);
  // }
  // return TupleSet::make(*expTable);
}

// std::shared_ptr<arrow::RecordBatch>
void
BloomFilterUseKernel::filterRecordBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                        const std::shared_ptr<BlockedBloomFilter> &bloomFilter,
                                        const std::shared_ptr<RecordBatchHasher> &hasher) {
    int64_t batchSize = recordBatch->num_rows();
    uint8_t* bitVector = (uint8_t*) malloc(batchSize);

    // hash
    uint64_t* hashes = (uint64_t*) malloc(sizeof(uint64_t) * batchSize);
    hasher->hash(recordBatch, hashes);
    // filter to get bitvec
    clock_t start, end;
    start = clock();
    bloomFilter->Find(hasher->getHardwareFlags(), batchSize, hashes, bitVector);
    end = clock();
    total_time += end - start;
    free(hashes);
  
    // apply the bitvector to get filtered output
    // auto selectBuffer = std::make_unique<arrow::Buffer>(bitVector, arrow::bit_util::BytesForBits(batchSize));
    // arrow::ArrayData selectArrayData(arrow::boolean(), batchSize, {nullptr, std::move(selectBuffer)});
    // auto expDatum = arrow::compute::Filter(arrow::Datum(recordBatch), arrow::Datum(selectArrayData));
    free(bitVector);
    // if (!expDatum.ok()) {
    //    assert(false);
    // }
    // return (*expDatum).record_batch();
}