#pragma once

/**
 * An index into an arrow chunked array
 */
class ColumnIndex {
public:
  ColumnIndex(int chunk, long chunkIndex);

  void setChunk(int chunk);

  void setChunkIndex(long chunkIndex);

  [[nodiscard]] int getChunk() const;

  [[nodiscard]] long getChunkIndex() const;

private:
  int chunk_;
  long chunkIndex_;
};