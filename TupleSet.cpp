#include "TupleSet.hpp"
#include "Schema.hpp"
#include <sstream>
#include <iomanip>
#include "Util.hpp"

TupleSet::TupleSet(std::shared_ptr<arrow::Table> table)
    : table_(std::move(table)) {}

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::Table> &table) {
    return std::make_shared<TupleSet>(table);
}

std::shared_ptr<TupleSet> TupleSet::make(const std::vector<std::shared_ptr<Column>>& columns) {
    std::vector<std::shared_ptr<::arrow::ChunkedArray>> arrays;
    std::vector<std::shared_ptr<::arrow::Field>> fields;
    for(const auto &column: columns){
        arrays.push_back(column->getArrowArray());
        fields.push_back(std::make_shared<::arrow::Field>(column->getName(), column->type()));
    }
    auto schema = std::make_shared<::arrow::Schema>(fields);
    return make(schema, arrays);
}

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::Schema>& schema) {
    auto columns = Schema::make(schema)->makeColumns();
    return make(columns);
}

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::Schema>& schema,
                                         const std::vector<std::shared_ptr<arrow::Array>>& values){
    auto table = arrow::Table::Make(schema, values);
    return std::make_shared<TupleSet>(table);
}

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::Schema>& schema,
                                         const std::vector<std::shared_ptr<arrow::ChunkedArray>>& values){
    auto table = arrow::Table::Make(schema, values);
    return std::make_shared<TupleSet>(table);
}

std::shared_ptr<TupleSet> TupleSet::make(const std::shared_ptr<arrow::Schema>& schema,
                                         const std::vector<std::shared_ptr<Column>>& columns) {
    auto chunkedArrays = Column::columnVectorToArrowChunkedArrayVector(columns);
    auto arrowTable = ::arrow::Table::Make(schema, chunkedArrays);
    return std::make_shared<TupleSet>(arrowTable);
}

std::shared_ptr<TupleSet>
TupleSet::make(const std::vector<std::shared_ptr<arrow::RecordBatch>> &recordBatches) {
    auto expTable = arrow::Table::FromRecordBatches(recordBatches);
    if (!expTable.ok()) {
        assert(false);
    }
    return make(*expTable);
}

std::shared_ptr<TupleSet>
TupleSet::make(const std::shared_ptr<arrow::csv::TableReader> &tableReader) {
    auto result = tableReader->Read();
    if (!result.ok()) {
        assert(false);
    }

    auto tupleSet = std::make_shared<TupleSet>();
    auto table = result.ValueOrDie();
    tupleSet->table_ = table;

    assert(tupleSet);
    assert(tupleSet->table_);
    assert(tupleSet->table_->ValidateFull().ok());

    return tupleSet;
}

std::shared_ptr<TupleSet> TupleSet::makeWithNullTable() {
    return std::make_shared<TupleSet>();
}

std::shared_ptr<TupleSet> TupleSet::makeWithEmptyTable() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto schema = arrow::schema(fields);
    return make(schema);
}

bool TupleSet::valid() const {
    return table_ != nullptr;
}

bool TupleSet::validate() const {
    if(valid())
        return table_->ValidateFull().ok();
    else
        return true;
}

void TupleSet::clear() {
    table_ = nullptr;
}

int64_t TupleSet::numRows() const {
    if (table_) {
        return table_->num_rows();
    } else {
        return 0;
    }
}

int TupleSet::numColumns() const {
    if (table_) {
        return table_->num_columns();
    } else {
        return 0;
    }
}

size_t TupleSet::size() const {
    size_t size = 0;
    for (int i = 0; i < numColumns(); i++) {
        size += getColumnByIndex(i)->size();
    }
    return size;
}

std::shared_ptr<arrow::Schema> TupleSet::schema() const {
    return table_->schema();
}

std::shared_ptr<arrow::Table> TupleSet::table() const {
    return table_;
}

void TupleSet::table(const std::shared_ptr<arrow::Table> &table) {
    table_ = table;
}

std::shared_ptr<TupleSet>
TupleSet::concatenate(const std::vector<std::shared_ptr<TupleSet>> &tupleSets) {
    // remove empty tupleSets
    std::vector<std::shared_ptr<TupleSet>> nonEmptyTupleSets;
    for (const auto &tupleSet: tupleSets) {
        // check valid first
        if (!tupleSet->valid()){
            assert(false);
        }
        if (tupleSet->numRows() > 0) {
        nonEmptyTupleSets.emplace_back(tupleSet);
        }
    }

    // if no or only one empty tupleSet
    if (nonEmptyTupleSets.empty()) {
        return tupleSets[0];
    }
    if (nonEmptyTupleSets.size() == 1) {
        return nonEmptyTupleSets[0];
    }

    // real concatenation occurs here
    const auto &schema = nonEmptyTupleSets[0]->schema();
    std::vector<std::shared_ptr<arrow::Table>> tables{nonEmptyTupleSets[0]->table_};
    for (size_t i = 1; i < nonEmptyTupleSets.size(); ++i) {
        // try to calibrate schema first
        auto expCalibratedTable = Util::calibrateSchema(nonEmptyTupleSets[i]->table_, schema);
        if (expCalibratedTable == nullptr) {
            assert(false);
        }
        tables.emplace_back(expCalibratedTable);
    }
    auto expConcatenatedTable = arrow::ConcatenateTables(tables);
    if (expConcatenatedTable.ok()) {
        return std::make_shared<TupleSet>(expConcatenatedTable.ValueOrDie());
    } else {
        assert(false);
    }
}

void TupleSet::append(const std::vector<std::shared_ptr<TupleSet>> &tupleSets) {
    auto tupleSetVector = std::vector{shared_from_this()};
    tupleSetVector.insert(tupleSetVector.end(), tupleSets.begin(), tupleSets.end());
    auto expected = concatenate(tupleSetVector);
    if (expected == nullptr)
        assert(false);
    else {
        this->table_ = expected->table_;
        return; // TODO: This seems to be how to return a void expected AFAICT, verify?
    }
}

void TupleSet::append(const std::shared_ptr<TupleSet> &tupleSet) {
    auto tupleSetVector = std::vector{tupleSet};
    return append(tupleSetVector);
}

std::shared_ptr<Column> TupleSet::getColumnByName(const std::string &name) const {
    auto columnArray = table_->GetColumnByName(name);
    if (columnArray == nullptr) {
        assert(false);
    } else {
        auto column = Column::make(name, columnArray);
        return column;
    }
}

std::shared_ptr<Column> TupleSet::getColumnByIndex(const int &columnIndex) const {
    auto columnName = table_->field(columnIndex)->name();
    auto columnArray = table_->column(columnIndex);
    if (columnArray == nullptr) {
        assert(false);
    } else {
        auto column = Column::make(columnName, columnArray);
        return column;
    }
}

std::shared_ptr<TupleSet>
TupleSet::projectExist(const std::vector<std::string> &columnNames) const {
    std::vector<std::shared_ptr<Column>> columns;
    for (const auto &columnName: columnNames) {
        const auto &expColumn = getColumnByName(columnName);
        if (expColumn != nullptr) {
            columns.emplace_back(expColumn);
        }
    }
    return make(columns);
}

std::shared_ptr<arrow::RecordBatch> TupleSet::projectExist(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                           const std::vector<std::string> &columnNames) {
  return projectExist(*recordBatch, columnNames);
}

std::shared_ptr<arrow::RecordBatch> TupleSet::projectExist(const arrow::RecordBatch &recordBatch,
                                                           const std::vector<std::string> &columnNames) {
  ::arrow::FieldVector projectFields;
  ::arrow::ArrayVector projectArrays;
  for (const auto &projectColumnName: columnNames) {
    auto projectField = recordBatch.schema()->GetFieldByName(projectColumnName);
    if (projectField != nullptr) {
      projectFields.emplace_back(projectField);
    }
    auto projectArray = recordBatch.GetColumnByName(projectColumnName);
    if (projectArray != nullptr) {
      projectArrays.emplace_back(projectArray);
    }
  }
  return ::arrow::RecordBatch::Make(arrow::schema(projectFields),
                                    recordBatch.num_rows(),
                                    projectArrays);
}

std::shared_ptr<TupleSet> TupleSet::project(const std::vector<int> &columnIds) const {
    const auto &expTable = table_->SelectColumns(columnIds);
    if (!expTable.ok()) {
        assert(false);
    }
    return make(expTable.ValueOrDie());
}

void TupleSet::renameColumns(const std::vector<std::string>& columnNames){
    if(valid()){
        auto expectedTable = table_->RenameColumns(columnNames);
        if(expectedTable.ok())
            table_ = *expectedTable;
        else
            assert(false);
    }
    return;
}

std::shared_ptr<TupleSet>
TupleSet::renameColumnsWithNewTupleSet(const std::vector<std::string>& columnNames) {
    auto expTable = table_->RenameColumns(columnNames);
    if (!expTable.ok()) {
        assert(false);
    }
    return make(*expTable);
}

void TupleSet::renameColumns(const std::unordered_map<std::string, std::string> &columnRenames) {
    if (valid() && !columnRenames.empty()) {
        auto columnNames = table_->ColumnNames();
        // collect ids
        std::unordered_map<std::string, uint> columnNameIds;
        for (uint i = 0; i < columnNames.size(); ++i) {
            columnNameIds.emplace(columnNames[i], i);
        }
        // rename
        for (const auto &renameIt: columnRenames) {
            const auto &oldName = renameIt.first;
            const auto &newName = renameIt.second;
            const auto &columnIdIt = columnNameIds.find(oldName);
            if (columnIdIt == columnNameIds.end()) {
                assert(false);
            }
            columnNames[columnIdIt->second] = newName;
        }
        auto expectedTable = table_->RenameColumns(columnNames);
        if(expectedTable.ok())
            table_ = *expectedTable;
        else
            assert(false);
    }
    return;
}

void TupleSet::combine() {
    auto expectedTable = table_->CombineChunks();
    if(expectedTable.ok())
        table_ = *expectedTable;
    else
        assert(false);
    return;
}

std::string TupleSet::showString() {
    return showString(TupleSetShowOptions(TupleSetShowOrientation::ColumnOriented));
}

/**
 * FIXME: This is really slow
 * @param options
 * @return
 */
std::string TupleSet::showString(TupleSetShowOptions options) {
    if (!valid()) {
        return "<empty>";
    } else {
        if (options.getOrientation() == TupleSetShowOrientation::ColumnOriented) {
            auto ss = std::stringstream();
            arrow::Status arrowStatus = ::arrow::PrettyPrint(*table_, 0, &ss);
            if (!arrowStatus.ok()) {
                // FIXME: How to handle this? How can pretty print fail? RAM? Maybe just abort?
                throw std::runtime_error(arrowStatus.detail()->ToString());
            }
            return ss.str();
        } else {
            int width = 120;
            std::stringstream ss;
            ss << std::endl;
            ss << std::left << std::setw(width) << std::setfill('-') << "" << std::endl;
            int columnWidth = table_->num_columns() == 0 ? width : width / table_->num_columns();
            // Column names
            for (const auto &field: table_->schema()->fields()) {
                ss << std::left << std::setw(columnWidth) << std::setfill(' ');
                ss << "| " + field->name();
            }
            ss << std::endl;
            // Column types
            for (const auto &field: table_->schema()->fields()) {
                ss << std::left << std::setw(columnWidth) << std::setfill(' ');
                auto fieldType = field->type();
                if (fieldType) {
                    ss << "| " + field->type()->ToString();
                } else {
                    ss << "| unknown";
                }
            }
            ss << std::endl;
            ss << std::left << std::setw(width) << std::setfill('-') << "" << std::endl;
            // Data
            int rowCounter = 0;
            for (int rowIndex = 0; rowIndex < table_->num_rows(); ++rowIndex) {
                if (rowCounter >= options.getMaxNumRows())
                    break;
                for (int columnIndex = 0; columnIndex < table_->num_columns(); ++columnIndex) {
                    auto column = this->getColumnByIndex(columnIndex);
                    auto value = column->element(rowIndex);
                    ss << std::left << std::setw(columnWidth) << std::setfill(' ');
                    ss << "| " + value->toString();
                }
                ss << std::endl;
                rowCounter++;
            }
            // Shape
            ss << std::left << std::setw(width) << std::setfill('-') << "" << std::endl;
            ss << table_->num_columns() << " cols x " << table_->num_rows() << " rows";
            if (rowCounter < table_->num_rows()) {
                ss << " (showing only top " << rowCounter << " rows)";
            }
            ss << std::endl;
            return ss.str();
        }
    }
}

std::string TupleSet::toString() const {
    std::string s;
    if(valid())
        return s;
    else
        return s;
}

std::shared_ptr<arrow::Scalar> TupleSet::visit(const std::function<std::shared_ptr<arrow::Scalar>(
        std::shared_ptr<arrow::Scalar>, arrow::RecordBatch &)> &fn) {
    arrow::Status arrowStatus;
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::TableBatchReader reader(*table_);
//  reader.set_chunksize(tuple::DefaultChunkSize);
    arrowStatus = reader.ReadNext(&batch);
    std::shared_ptr<arrow::Scalar> result;
    while (arrowStatus.ok() && batch) {
        result = fn(result, *batch);
        arrowStatus = reader.ReadNext(&batch);
    }
    return result;
}

std::vector<std::shared_ptr<arrow::Table>>
TupleSet::tupleSetVectorToArrowTableVector(const std::vector<std::shared_ptr<TupleSet>> &tupleSets) {
    std::vector<std::shared_ptr<arrow::Table>> tableVector;
    tableVector.reserve(tupleSets.size());
    for (const auto &tupleSet: tupleSets) {
        if (tupleSet->valid())
        tableVector.push_back(tupleSet->table_);
    }
    return tableVector;
}