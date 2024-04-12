#include "ColumnIndex.hpp"

ColumnIndex::ColumnIndex(int chunk, long chunkIndex)
    : chunk_(chunk), chunkIndex_(chunkIndex) {}

void ColumnIndex::setChunk(int chunk) {
    chunk_ = chunk;
}

void ColumnIndex::setChunkIndex(long chunkIndex) {
    chunkIndex_ = chunkIndex;
}

int ColumnIndex::getChunk() const {
    return chunk_;
}

long ColumnIndex::getChunkIndex() const {
    return chunkIndex_;
}