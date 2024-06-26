#include "ColumnIterator.hpp"

ColumnIterator::ColumnIterator() :
	index_(ColumnIndex(0, 0)) {}

ColumnIterator::ColumnIterator(std::shared_ptr<::arrow::ChunkedArray> chunkedArray, int chunk, long chunkIndex) :
	chunkedArray_(std::move(chunkedArray)), index_(ColumnIndex(chunk, chunkIndex)) {}

void ColumnIterator::advance() {
    if(index_.getChunk() < chunkedArray_->num_chunks()) {
        // Not at end yet
	    if (index_.getChunkIndex() < chunkedArray_->chunk(index_.getChunk())->length() - 1) {
	        // Chunk index not at end of chunk, advance chunk index
	        index_.setChunkIndex(index_.getChunkIndex() + 1);
	    } else {
	        // Chunk index at end of chunk, advance chunk and reset chunk index
	        index_.setChunk(index_.getChunk() + 1);
	        index_.setChunkIndex(0);
            // Advance again if we're not at the end and ve've advanced to an empty chunk
	        if(index_.getChunk() < chunkedArray_->num_chunks() && chunkedArray_->chunk(index_.getChunk())->length() == 0)
	            advance();
	    }
    }
}

ColumnIterator::this_type &ColumnIterator::operator++() {
    advance();
    return *this;
}

ColumnIterator::this_type ColumnIterator::operator++(int) {
    auto iterator = *this;
    ++(*this);
    return iterator;
}

ColumnIterator::value_type ColumnIterator::value() const {
  return getScalar();
}

ColumnIterator::value_type ColumnIterator::operator*() const {
    if(index_.getChunk() >= chunkedArray_->num_chunks() || index_.getChunkIndex() >= chunkedArray_->chunk(index_.getChunk())->length()){
        assert(false);
    }
    return getScalar();
}

ColumnIterator::pointer ColumnIterator::operator->() const {
    auto expScalar = getScalar();
    if (expScalar == nullptr) {
        assert(false);
    }
    return std::make_shared<value_type>(expScalar);
}

bool ColumnIterator::operator==(const ColumnIterator::this_type &other) const {
    return index_.getChunk() == other.index_.getChunk() && index_.getChunkIndex() == other.index_.getChunkIndex();
}

bool ColumnIterator::operator!=(const ColumnIterator::this_type &other) const {
    return !(*this == other);
}

ColumnIterator::this_type ColumnIterator::operator-(const ColumnIterator::difference_type &other) {
    return {chunkedArray_, index_.getChunk() - other.index_.getChunk(), index_.getChunkIndex() - other.index_.getChunkIndex()};
}

std::shared_ptr<arrow::Scalar> ColumnIterator::getArrowScalar() const {
    // Need to cast to the array type to be able to use the element accessors
    if (chunkedArray_->type()->id() == arrow::int64()->id()) {
        auto typedArray = std::dynamic_pointer_cast<arrow::Int64Array>(chunkedArray_->chunk(index_.getChunk()));
        auto value = typedArray->Value(index_.getChunkIndex());
        auto arrowScalar = arrow::MakeScalar(value);
        return arrowScalar;
    } else if (chunkedArray_->type()->id() == arrow::utf8()->id()) {
        auto typedArray = std::dynamic_pointer_cast<arrow::StringArray>(chunkedArray_->chunk(index_.getChunk()));
        auto value = typedArray->GetString(index_.getChunkIndex());
        auto arrowScalar = arrow::MakeScalar(value);
        return arrowScalar;
    } else {
        assert(false);
    }
}

ColumnIterator::value_type ColumnIterator::getScalar() const {
    auto expArrowScalar = getArrowScalar();
    if (expArrowScalar == nullptr) {
        assert(false);
    }
    return Scalar::make(expArrowScalar);
}