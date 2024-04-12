#include "RecordBatchHasher.hpp"

std::shared_ptr<RecordBatchHasher>
RecordBatchHasher::make(const std::shared_ptr<arrow::Schema> &schema,
                        const std::vector<std::string> &columnNames) {
    auto hasher = std::make_shared<RecordBatchHasher>();

    // temp stack
    auto status = hasher->tempStack_.Init(arrow::default_memory_pool(), 64 * MiniBatchSize_);
    if (!status.ok()) {
        assert(false);
    }

    hasher->encodeCtx_.hardware_flags = arrow::internal::CpuInfo::GetInstance()->hardware_flags();
    hasher->encodeCtx_.stack = &hasher->tempStack_;

    // key indices
    size_t numKeys = columnNames.size();
    hasher->keyIndices_.resize(numKeys);
    for (size_t i = 0; i < numKeys; ++i) {
        int keyIndex = schema->GetFieldIndex(columnNames[i]);
        if (keyIndex == -1) {
            assert(false);
        }
        hasher->keyIndices_[i] = keyIndex;
    }

    // col metadata
    hasher->colMetadata_.resize(numKeys);
    for (size_t i = 0; i < numKeys; ++i) {
        int icol = hasher->keyIndices_[i];
        const auto &type = schema->field(icol)->type();
        if (type->id() == arrow::Type::DICTIONARY) {
            auto bit_width = arrow::internal::checked_cast<const arrow::FixedWidthType&>(*type).bit_width();
            ARROW_DCHECK(bit_width % 8 == 0);
            hasher->colMetadata_[i] = arrow::compute::KeyColumnMetadata(true, bit_width / 8);
        } else if (type->id() == arrow::Type::BOOL) {
            hasher->colMetadata_[i] = arrow::compute::KeyColumnMetadata(true, 0);
        } else if (is_fixed_width(type->id())) {
            hasher->colMetadata_[i] = arrow::compute::KeyColumnMetadata(true,
            arrow::internal::checked_cast<const arrow::FixedWidthType&>(*type).bit_width() / 8);
        } else if (is_binary_like(type->id())) {
            hasher->colMetadata_[i] = arrow::compute::KeyColumnMetadata(false, sizeof(uint32_t));
        } else {
            assert(false);
        }
    }

    // cols
    hasher->cols_.resize(numKeys);

    return hasher;
}

void RecordBatchHasher::hash(const std::shared_ptr<arrow::RecordBatch> &recordBatch, uint32_t *hashes) {
    // make key cols
    makeKeyColumns(recordBatch);

    // compute hash
    arrow::compute::Hashing32::HashMultiColumn(cols_, &encodeCtx_, hashes);
}

void RecordBatchHasher::hash(const std::shared_ptr<arrow::RecordBatch> &recordBatch, uint64_t *hashes) {
    // make key cols
    makeKeyColumns(recordBatch);

    // compute hash
    arrow::compute::Hashing64::HashMultiColumn(cols_, &encodeCtx_, hashes);
}

void RecordBatchHasher::makeKeyColumns(const std::shared_ptr<arrow::RecordBatch> &recordBatch) {
  int64_t numRows = recordBatch->num_rows();
  arrow::compute::ExecBatch execBatch(*recordBatch);

  // make cols
  for (size_t i = 0; i < keyIndices_.size(); ++i) {
    int icol = keyIndices_[i];
    const uint8_t* nonNulls = nullptr;
    if (execBatch[icol].array()->buffers[0] != NULLPTR) {
      nonNulls = execBatch[icol].array()->buffers[0]->data();
    }
    const uint8_t* fixedLen = execBatch[icol].array()->buffers[1]->data();
    const uint8_t* varLen = nullptr;
    if (!colMetadata_[i].is_fixed_length) {
      varLen = execBatch[icol].array()->buffers[2]->data();
    }

    int64_t offset = execBatch[icol].array()->offset;
    auto colBase = arrow::compute::KeyColumnArray(colMetadata_[i], offset + numRows, nonNulls, fixedLen, varLen);
    cols_[i] = colBase.Slice(offset, numRows);
  }
}

int64_t RecordBatchHasher::getHardwareFlags() const {
  return encodeCtx_.hardware_flags;
}