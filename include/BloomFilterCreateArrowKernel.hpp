#pragma once

#include <vector>
#include <string>
#include <memory>
#include "BloomFilter.hpp"
#include "TupleSet.hpp"

class BloomFilterCreateArrowKernel {
public:
    BloomFilterCreateArrowKernel(const std::vector<std::string> &columnNames);
    BloomFilterCreateArrowKernel() = default;
    BloomFilterCreateArrowKernel(const BloomFilterCreateArrowKernel&) = default;
    BloomFilterCreateArrowKernel& operator=(const BloomFilterCreateArrowKernel&) = default;
    ~BloomFilterCreateArrowKernel() = default;

    static std::shared_ptr<BloomFilterCreateArrowKernel> make(const std::vector<std::string> &columnNames);

    void buildBloomFilter();
    std::shared_ptr<BlockedBloomFilter> getBloomFilter() const;

    void bufferTupleSet(const std::shared_ptr<TupleSet> &tupleSet);

    void clear();

protected:
    std::vector<std::string> columnNames_;
    std::shared_ptr<TupleSet> receivedTupleSet_;
private:
    std::shared_ptr<BlockedBloomFilter> bloomFilter_;

// caf inspect
public:
    template <class Inspector>
    friend bool inspect(Inspector& f, BloomFilterCreateArrowKernel& kernel) {
        return f.object(kernel).fields(f.field("columnNames", kernel.columnNames_));
    }
};