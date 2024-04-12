#include "Util.hpp"

std::shared_ptr<arrow::Table>
Util::calibrateSchema(const std::shared_ptr<arrow::Table> &table, const std::shared_ptr<arrow::Schema> &targetSchema) {
    // schemas are identical, just return
    if (table->schema()->Equals(targetSchema)) {
        return table;
    }
    // different number of fields
    if (table->schema()->num_fields() != targetSchema->num_fields()) {
        assert(false);
    }
    // schemas are not identical, try if we can rearrange columns to make them same
    std::unordered_map<std::string, std::shared_ptr<arrow::Field>> targetFieldMap;
    std::unordered_map<std::string, int> targetFieldIdMap;
    for (int i = 0; i < targetSchema->num_fields(); ++i) {
        const auto &field = targetSchema->field(i);
        targetFieldMap.emplace(field->name(), field);
        targetFieldIdMap.emplace(field->name(), i);
    }
    arrow::FieldVector rearrangedFields{(size_t) table->num_columns()};
    arrow::ChunkedArrayVector rearrangedColumns{(size_t) table->num_columns()};
    for (int i = 0; i < table->num_columns(); ++i) {
        const auto &field = table->schema()->field(i);
        const auto &column = table->column(i);
        // try to get field id
        auto targetFieldIt = targetFieldMap.find(field->name());
        if (targetFieldIt == targetFieldMap.end()) {
            assert(false);
        }
        const auto &targetField = targetFieldIt->second;
        if (field->type()->id() != targetField->type()->id()) {
            assert(false);
        }
        int fieldId = targetFieldIdMap[field->name()];
        rearrangedFields[fieldId] = field;
        rearrangedColumns[fieldId] = column;
    }
    // rearrange is successful
    return arrow::Table::Make(arrow::schema(rearrangedFields), rearrangedColumns);
}