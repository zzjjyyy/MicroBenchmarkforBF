#include <iostream>
#include <vector>
#include <string>
#include <set>
#include <random>
#include "Sample.hpp"
#include "BloomFilterCreateArrowKernel.hpp"
#include "BloomFilterUseKernel.hpp"
#include "RecordBatchHasher.hpp"

clock_t total_time = 0;

void runArrowBloomFilterHashJoinCompare(const std::shared_ptr<TupleSet> &leftTupleSet, const std::shared_ptr<TupleSet> &rightTupleSet,
                                        const std::vector<std::string> &leftColumnNames, const std::vector<std::string> &rightColumnNames,
                                        const std::set<std::string> &neededColumnNames, std::vector<int64_t> &bfOutSizes,
                                        std::vector<int64_t> &hjOutSizes) {
    total_time = 0;
    // clock_t start, end;
    // bloom filter, basically copied from functions above
    bool bfOutRecorded = false;
    // create
    auto kernel = BloomFilterCreateArrowKernel::make(leftColumnNames);
    kernel->bufferTupleSet(leftTupleSet);
    kernel->buildBloomFilter();
    auto bloomFilter = kernel->getBloomFilter();
    // use
    auto expHasher = RecordBatchHasher::make(rightTupleSet->schema(), rightColumnNames);
    if (expHasher == nullptr) {
        assert(false);
    }
    // start = clock();
    // auto expFilteredTupleSet =
        BloomFilterUseKernel::filter(rightTupleSet,
                                     bloomFilter,
                                     expHasher);
    // if (expFilteredTupleSet == nullptr) {
    //     assert(false);
    // }
    // if (!bfOutRecorded) {
    //     bfOutSizes.emplace_back(expFilteredTupleSet->numRows());
    //     bfOutRecorded = true;
    // }
    // end = clock();
    std::cout << double(total_time) / CLOCKS_PER_SEC << std::endl;
}

int main() {
    std::vector<std::string> leftColumnNames{"c_0"};
    std::vector<std::string> rightColumnNames{"c_0"};
    std::set<std::string> neededColumnNames{"c_0", "c_1", "c_2"};
    auto dist1 = std::uniform_int_distribution(0, 1);
    auto dist10 = std::uniform_int_distribution(0, 10);
    auto dist100 = std::uniform_int_distribution(0, 100);
    auto dist1000 = std::uniform_int_distribution(0, 1000);
    auto dist10000 = std::uniform_int_distribution(0, 10000);
    auto dist100000 = std::uniform_int_distribution(0, 100000);
    auto dist1000000 = std::uniform_int_distribution(0, 1000000);
    auto dist10000000 = std::uniform_int_distribution(0, 10000000);
    auto dist100000000 = std::uniform_int_distribution(0, 100000000);

    auto tupleSet1 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1, dist1);
    auto tupleSet10 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10, dist10);
    auto tupleSet100 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100, dist100);
    auto tupleSet1000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000, dist1000);
    auto tupleSet10000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000, dist10000);
    
    auto tupleSet100000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000, dist100000);
    auto tupleSet1000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 1000000, dist1000000);
    auto tupleSet10000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 10000000, dist10000000);
    auto tupleSet100000000 = Sample::sampleCxRInt<long, arrow::Int64Type>(10, 100000000, dist100000000);
    
    std::vector<int64_t> bfOutSizes, hjOutSizes;
    runArrowBloomFilterHashJoinCompare(tupleSet1, tupleSet100000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    runArrowBloomFilterHashJoinCompare(tupleSet10, tupleSet100000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    runArrowBloomFilterHashJoinCompare(tupleSet100, tupleSet100000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    runArrowBloomFilterHashJoinCompare(tupleSet1000, tupleSet100000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    runArrowBloomFilterHashJoinCompare(tupleSet10000, tupleSet100000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    runArrowBloomFilterHashJoinCompare(tupleSet100000, tupleSet100000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    // runArrowBloomFilterHashJoinCompare(tupleSet1000000, tupleSet10000000, leftColumnNames, rightColumnNames, neededColumnNames, bfOutSizes, hjOutSizes);
    return 0;
}