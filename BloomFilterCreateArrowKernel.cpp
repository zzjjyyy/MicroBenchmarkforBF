#include "BloomFilterCreateArrowKernel.hpp"
#include "RecordBatchHasher.hpp"
#include "BloomFilter.hpp"

BloomFilterCreateArrowKernel::BloomFilterCreateArrowKernel(const std::vector<std::string> &columnNames)
    : columnNames_(columnNames){}

std::shared_ptr<BloomFilterCreateArrowKernel>
BloomFilterCreateArrowKernel::make(const std::vector<std::string> &columnNames) {
    return std::make_shared<BloomFilterCreateArrowKernel>(columnNames);
}

void BloomFilterCreateArrowKernel::buildBloomFilter() {
    // check
    if (receivedTupleSet_ == nullptr) {
      assert(false);
    }
    bloomFilter_ = std::make_shared<BlockedBloomFilter>();
    // build bloom filter
    // make hasher
    auto expHasher = RecordBatchHasher::make(receivedTupleSet_->schema(), columnNames_);
    if (expHasher == nullptr) {
        assert(false);
    }
  
  // init bloom filter
  auto hardwareFlags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
  auto builder = BloomFilterBuilder::Make(BloomFilterBuildStrategy::SINGLE_THREADED);
  auto res = builder->Begin(1, hardwareFlags, arrow::default_memory_pool(), receivedTupleSet_->numRows(), 0, bloomFilter_.get());
  if (!res.ok()) {
    assert(false);
  }
  
  // push batches into bloom filter
  arrow::TableBatchReader reader{*receivedTupleSet_->table()};
  auto expRecordBatch = reader.Next();
  if (!expRecordBatch.ok()) {
    assert(false);
  }
  auto recordBatch = *expRecordBatch;
  while (recordBatch) {
    // use 32/64-bit hashes according to the initialized bloom filter
    uint64_t *hashes = (uint64_t*) malloc(sizeof(uint64_t) * recordBatch->num_rows());
    expHasher->hash(recordBatch, hashes);
    res = builder->PushNextBatch(0, recordBatch->num_rows(), hashes);
    free(hashes);
    if (!res.ok()) {
      assert(false);
    }

    // next batch
    expRecordBatch = reader.Next();
    if (!expRecordBatch.ok()) {
      assert(false);
    }
    recordBatch = *expRecordBatch;
  }
  return;
}

std::shared_ptr<BlockedBloomFilter> BloomFilterCreateArrowKernel::getBloomFilter() const {
  return bloomFilter_;
}

void BloomFilterCreateArrowKernel::bufferTupleSet(const std::shared_ptr<TupleSet> &tupleSet) {
  // buffer tupleSet
  if (receivedTupleSet_ == nullptr) {
    receivedTupleSet_ = tupleSet;
  }
  else {
    // here we should use "concatenate" instead of "append" to avoid modification on the original tupleSet,
    // because the original one is also passed to downstream operators
    auto expConcatenatedTupleSet = TupleSet::concatenate({receivedTupleSet_, tupleSet});
    if (expConcatenatedTupleSet == nullptr) {
      assert(false);
    }
    receivedTupleSet_ = expConcatenatedTupleSet;
  }
  return;
}

void BloomFilterCreateArrowKernel::clear() {
  bloomFilter_.reset();
}